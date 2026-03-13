import contextlib
import io
import inspect
import logging
import multiprocessing
import os
import queue
import signal
import sys
import time
import traceback

import dill
import dxspaces
import rexec.remote_obj
import zmq

HEARTBEAT_FRAME = b"__REXEC_HEARTBEAT__"
STREAM_CANCEL_FRAME = b"__REXEC_CANCEL__"

STREAM_EVENT_START = "START"
STREAM_EVENT_DATA = "DATA"
STREAM_EVENT_END = "END"
STREAM_EVENT_ERROR = "ERROR"


class _StreamTextEmitter(io.TextIOBase):
    def __init__(self, emit_fn):
        self._emit_fn = emit_fn

    def write(self, text):
        if not text:
            return 0
        self._emit_fn(text)
        return len(text)

    def flush(self):
        return None



def _pid_is_alive(pid):
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True

def _terminate_pid(pid):
    if pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        if not _pid_is_alive(pid):
            return
        time.sleep(0.01)
    if _pid_is_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return

# Worker function to execute the target function in a child process, capture stdout/stderr, 
# and send events back to the server via a multiprocessing queue.
def _worker_execute(fn_frame, args_frame, event_queue):
    stdout_emitter = _StreamTextEmitter(
        lambda text: event_queue.put(("data", "stdout", text))
    )
    stderr_emitter = _StreamTextEmitter(
        lambda text: event_queue.put(("data", "stderr", text))
    )

    with contextlib.redirect_stdout(stdout_emitter), contextlib.redirect_stderr(stderr_emitter):
        try:
            fn = dill.loads(fn_frame)
            args = dill.loads(args_frame)
            ret = fn(*args)
            if inspect.isgenerator(ret):
                for item in ret:
                    event_queue.put(("data", "yield", item))
            elif ret is not None:
                event_queue.put(("data", "return", ret))
        except Exception as exc:
            event_queue.put(
                ("error", f"An unexpected error occurred: {exc}", traceback.format_exc())
            )
        finally:
            event_queue.put(("done", None, None))



class RExecServer:
    rexec.remote_obj.DSDataObj.ctx = "server"

    # Initialize the RExecServer with ZeroMQ connection to the broker, 
    # heartbeat configuration, and multiprocessing context for executing functions in child processes.
    def __init__(self, args):
        self.zmq_addr = "tcp://" + args.broker_addr + ":" + args.broker_port
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.DEALER)
        self.heartbeat_interval = max(0.0, float(args.heartbeat_interval))
        
        # Use a multiprocessing context to manage execution processes, 
        # allowing for safe process spawning and termination across different platforms.
        # The start method is resolved based on env var and platform-specific considerations to ensure compatibility
        self.mp_ctx = multiprocessing.get_context(self._resolve_mp_start_method())
        # Value to track the active execution process's PID for interrupting if needed.
        self.active_exec_pid = self.mp_ctx.Value("i", 0)
        # Buffer for incoming messages received while an execution is active and the server is waiting for events; 
        # to be processed after the current execution completes or is interrupted.
        self.pending_messages = []

        # Set server identity from server container's env var
        # ---
        user_id = os.environ.get("REXEC_USER_ID") # env var for server identity; set during deployment
        if user_id:
            # set zmq.DEALER socket identity
            self.zmq_socket.setsockopt(zmq.IDENTITY, user_id.encode("utf-8"))
            logging.info("Set server identity to %s", user_id)
        else:
            logging.warning("REXEC_USER_ID not set; server identity will be random.")

        # Connect to broker and report its identity
        self.zmq_socket.connect(self.zmq_addr)
        logging.info(f"Connected to {self.zmq_addr}")
        
        # If DataSpaces API address is provided, 
        # initialize the client and set it on the DSDataObj class for use in remote functions.
        if(args.dspaces_api_addr):
            dspaces_client = dxspaces.DXSpacesClient(args.dspaces_api_addr)
            logging.info("Connected to DataSpaces API.")
            rexec.remote_obj.DSDataObj.dspaces_client = dspaces_client



    # Utility
    # ---
    @staticmethod
    def _summarize_value(value, max_len=200):
        try:
            rep = repr(value)
        except Exception:
            rep = "<unreprable>"
        if len(rep) > max_len:
            rep = rep[: max_len - 3] + "..."
        return f"{type(value).__name__}: {rep}"

    @staticmethod
    def _split_envelope(frames):
        for idx, frame in enumerate(frames):
            if frame == b"":
                envelope = frames[:idx]
                body = frames[idx + 1 :]
                return envelope, idx, body
        return [], None, frames

    def _send_heartbeat(self):
        if self.heartbeat_interval <= 0:
            return
        try:
            # Empty delimiter keeps framing consistent for broker parsing.
            self.zmq_socket.send_multipart([b"", HEARTBEAT_FRAME], zmq.DONTWAIT)
            logging.debug("Sent heartbeat to broker.")
        except zmq.Again:
            logging.debug("Heartbeat dropped due to backpressure.")
        except zmq.ZMQError as exc:
            logging.warning("Failed to send heartbeat: %s", exc)

    def _send_payload(self, envelope, payload):
        if envelope:
            self.zmq_socket.send_multipart(envelope + [b"", payload])
        else:
            self.zmq_socket.send(payload)

    def _send_stream_event(self, envelope, seq, event_type, **kwargs):
        event = {
            "type": event_type,
            "sequence": seq,
        }
        event.update(kwargs) # merge additional event data into the event dict
        payload = dill.dumps(event)
        self._send_payload(envelope, payload)

    @staticmethod
    def _decode_cancel_text(body):
        # body format from broker: (__REXEC_CANCEL__, [reason_text])
        # return decoded [reason_text] if provided
        if len(body) < 2:
            return ""
        return body[-1].decode("utf-8", errors="replace").strip()

    @staticmethod
    def _is_keyboard_interrupt_text(text):
        normalized = (text or "").strip().lower()
        return normalized in { "keyboard_interrupt" }

    @staticmethod
    def _resolve_mp_start_method():
        """
        Resolve the multiprocessing start method based on environment variables and platform-specific considerations.
        """
        available = multiprocessing.get_all_start_methods()
        configured = os.environ.get("REXEC_MP_START_METHOD")
        if configured:
            if configured not in available:
                logging.warning(
                    "REXEC_MP_START_METHOD=%s is unavailable; falling back to defaults (%s).",
                    configured,
                    ", ".join(available),
                )
            else:
                logging.info("Using multiprocessing start method from env: %s", configured)
                return configured

        # On macOS, avoid forking after imports that may touch Objective-C runtime.
        if sys.platform == "darwin" and "spawn" in available:
            logging.info("Using multiprocessing start method: spawn (macOS safe default).")
            return "spawn"

        # Prefer "fork" on platforms where it's available for better performance
        if "fork" in available:
            logging.info("Using multiprocessing start method: fork.")
            return "fork"

        # Fallback to "spawn" if other methods are not available
        fallback = "spawn" if "spawn" in available else available[0]
        logging.info("Using multiprocessing start method fallback: %s", fallback)
        return fallback



    # Core function to trigger remote_func exec in a child process:
    # Handles stream events, and send back results;
    # with support for interrupting execution via cancel messages
    # ---
    def fn_exec(self, envelope, fn_frame, args_frame, fn_name):
        sequence = 0

        # Helper function to emit stream events to broker with consistent framing and sequencing
        def emit(event_type, **payload):
            nonlocal sequence
            sequence += 1
            self._send_stream_event(envelope, sequence, event_type, **payload)

        # Send the start event to client to indicate the beginning of execution
        emit(STREAM_EVENT_START, function=fn_name)

        stream_error = None
        done_received = False
        interrupt_requested = False
        event_queue = self.mp_ctx.Queue()

        # Start the execution in a separate process to allow interrupting and to avoid blocking the main server loop
        exec_process = self.mp_ctx.Process(
            target=_worker_execute,
            args=(fn_frame, args_frame, event_queue),
            daemon=True,
            name=f"rexec-exec-{fn_name}",
        )
        exec_process.start()
        with self.active_exec_pid.get_lock():
            self.active_exec_pid.value = int(exec_process.pid or 0)

        # Set up poller for zmq socket to listen for incoming control messages (e.g., cancel) while execution is in progress
        socket_poller = zmq.Poller()
        socket_poller.register(self.zmq_socket, zmq.POLLIN)

        try:
            while True:
                """
                1. Drain inbound control/data while execution process is active;          
                """
                # This allows the server to respond to cancel messages in a timely manner, 
                # even if the execution is currently busy processing.       
                while True:
                    socket_events = dict(socket_poller.poll(0))
                    if self.zmq_socket not in socket_events: break
                    incoming = self.zmq_socket.recv_multipart()
                    # Cancel message format:
                    # 0. [server_id](stripped out when arrive)
                    # 1. _envelope(client_id)
                    # 2. _delimiter_index(b"")
                    # 3. incoming_body("__REXEC_CANCEL__" + "keyboard_interrupt")
                    _envelope, _delimiter_index, incoming_body = self._split_envelope(incoming)
                    
                    # Check for cancel control message to interrupt execution; 
                    if incoming_body and incoming_body[0] == STREAM_CANCEL_FRAME:
                        # cancel_text should be "keyboard_interrupt"
                        cancel_text = self._decode_cancel_text(incoming_body)
                        if self._is_keyboard_interrupt_text(cancel_text):
                            interrupt_requested = True
                            logging.info(
                                "Keyboard interrupt requested for %s via cancel message: %r",
                                fn_name,
                                cancel_text,
                            )
                            # Terminate the execution process
                            _terminate_pid(int(exec_process.pid or 0))
                        else:
                            logging.debug(
                                "Ignoring cancel message while executing %s: %r",
                                fn_name,
                                cancel_text,
                            )
                        continue
                    self.pending_messages.append(incoming)

                """
                2. Process events from the remote_func execution process's event queue:
                """
                # Use a short timeout to periodically check event_queue
                try:
                    evt_type, evt_a, evt_b = event_queue.get(timeout=0.05)
                except queue.Empty:
                    if not exec_process.is_alive(): break
                    continue
                
                # Handle different types of events from the execution process:
                # "data" events include stdout/stderr output, yielded items from generators, and the final return value
                if evt_type == "data":
                    emit(STREAM_EVENT_DATA, channel=evt_a, data=evt_b)
                    # For return events, also log a summary of the returned value on the server side for visibility
                    if evt_a == "return":
                        logging.info(
                            "Returning from %s -> %s",
                            fn_name,
                            self._summarize_value(evt_b),
                        )
                # "error" events indicate an exception occurred during execution; 
                # capture the error message and details to send back to the client after cleanup
                elif evt_type == "error":
                    stream_error = {"message": evt_a, "details": evt_b}
                # "done" event indicates the execution process has completed its work
                elif evt_type == "done":
                    done_received = True
                    if not exec_process.is_alive():
                        break
        finally:
            # Ensure the execution process is terminated and cleaned up properly to avoid orphaned processes
            if exec_process.is_alive():
                _terminate_pid(int(exec_process.pid or 0))
            exec_process.join(timeout=0.2)
            with self.active_exec_pid.get_lock():
                self.active_exec_pid.value = 0
            try:
                event_queue.close()
            except Exception:
                pass
            try:
                event_queue.join_thread()
            except Exception:
                pass

        if stream_error:
            logging.error(
                "Streaming function %s failed:\n%s",
                fn_name,
                stream_error["details"],
            )
            emit(
                STREAM_EVENT_ERROR,
                message=stream_error["message"],
                details=stream_error["details"],
            )
            return

        if interrupt_requested:
            emit(STREAM_EVENT_END, cancelled=True, reason="keyboard interrupt")
            return

        if exec_process.exitcode not in (0, None) and not done_received:
            details = f"Execution process exited unexpectedly with code {exec_process.exitcode}."
            emit(STREAM_EVENT_ERROR, message=details, details=details)
            return

        emit(STREAM_EVENT_END)



    # Main loop to receive and execute functions, send back results; with heartbeat keepalive
    # ---
    
    def fn_recv_exec(self):
        # Set up poller for zmq socket with heartbeat handling
        poller = zmq.Poller()
        poller.register(self.zmq_socket, zmq.POLLIN)
        next_heartbeat = (
            time.monotonic() + self.heartbeat_interval
            if self.heartbeat_interval > 0
            else None
        )
        # Main loop to receive and execute functions, send back results; with heartbeat keepalive
        while True:
            if self.pending_messages:
                zmq_msg = self.pending_messages.pop(0)
            else:
                if next_heartbeat is None:
                    events = dict(poller.poll())
                else:
                    timeout_ms = max(
                        0,
                        int((next_heartbeat - time.monotonic()) * 1000),
                    )
                    events = dict(poller.poll(timeout_ms))

                if self.zmq_socket not in events:
                    if next_heartbeat is not None and time.monotonic() >= next_heartbeat:
                        self._send_heartbeat()
                        next_heartbeat = time.monotonic() + self.heartbeat_interval
                    continue

                zmq_msg = self.zmq_socket.recv_multipart()

            # received zmq_msg: envelope(client_id) + _delimiter_index(b"") + body(pfn, pargs)
            envelope, _delimiter_index, body = self._split_envelope(zmq_msg)

            # Basic validation of the message framing and handling of special control messages 
            # (e.g., cancel) before attempting to deserialize the function payload;
            if not body:
                logging.error("Invalid request framing: %s", zmq_msg)
                self._send_payload(envelope, dill.dumps("Invalid request framing."))
                continue
            if body[0] == STREAM_CANCEL_FRAME:
                # No active process to interrupt.
                cancel_text = self._decode_cancel_text(body)
                logging.debug("Ignoring cancel message while server is idle: %r", cancel_text)
                continue
            if len(body) < 2:
                logging.error("Invalid request framing: %s", zmq_msg)
                self._send_payload(envelope, dill.dumps("Invalid request framing."))
                continue
            
            # Extract the function and arguments frames from the message body
            fn_frame = body[0]
            args_frame = body[1]
            # And attempt to deserialize them using dill;
            try:
                fn = dill.loads(fn_frame)
                args = dill.loads(args_frame)
            except Exception as exc:
                logging.exception("Failed to deserialize invocation payload")
                self._send_payload(envelope, dill.dumps(f"Invalid function payload: {exc}"))
                continue
            # Get the func name
            fn_name = getattr(fn, "__name__", repr(fn))
            # Log the received function invocation for visibility;
            logging.info("Received function: %s ;with %d args", fn_name, len(args))

            # Execute the function,
            # and handle output stream results and interrupts
            self.fn_exec(envelope, fn_frame, args_frame, fn_name)

            if next_heartbeat is not None and time.monotonic() >= next_heartbeat:
                self._send_heartbeat()
                next_heartbeat = time.monotonic() + self.heartbeat_interval



    def run(self):
        try:
            logging.info(f"Start to receive functions...")
            self.fn_recv_exec()
        except KeyboardInterrupt:
            print("W: interrupt received, stopping rexec server...")
        finally:
            with self.active_exec_pid.get_lock():
                pid = int(self.active_exec_pid.value)
            if pid > 0:
                _terminate_pid(pid)

            self.zmq_socket.disconnect(self.zmq_addr)
            self.zmq_socket.close()
            self.zmq_context.destroy()
