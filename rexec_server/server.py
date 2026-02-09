import logging
import os
import time

import dill
import dxspaces
import rexec.remote_obj
import zmq

HEARTBEAT_FRAME = b"__REXEC_HEARTBEAT__"

class RExecServer:
    rexec.remote_obj.DSDataObj.ctx = "server"

    def __init__(self, args):
        self.zmq_addr = "tcp://" + args.broker_addr + ":" + args.broker_port
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.DEALER)
        self.heartbeat_interval = max(0.0, float(args.heartbeat_interval))

        # Set server identity from server container's env var
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
        
        if(args.dspaces_api_addr):
            dspaces_client = dxspaces.DXSpacesClient(args.dspaces_api_addr)
            logging.info("Connected to DataSpaces API.")
            rexec.remote_obj.DSDataObj.dspaces_client = dspaces_client

    @staticmethod
    def _summarize_value(value, max_len=200):
        try:
            rep = repr(value)
        except Exception:
            rep = "<unreprable>"
        if len(rep) > max_len:
            rep = rep[:max_len - 3] + "..."
        return f"{type(value).__name__}: {rep}"

    @staticmethod
    def _split_envelope(frames):
        for idx, frame in enumerate(frames):
            if frame == b"":
                envelope = frames[:idx]
                body = frames[idx + 1:]
                return envelope, idx, body
        return [], None, frames

    def _send_heartbeat(self) -> None:
        if self.heartbeat_interval <= 0:
            return
        try:
            # Empty delimiter keeps framing consistent for broker parsing.
            self.zmq_socket.send_multipart(
                [b"", HEARTBEAT_FRAME], zmq.DONTWAIT
            )
            logging.debug("Sent heartbeat to broker.")
        except zmq.Again:
            logging.debug("Heartbeat dropped due to backpressure.")
        except zmq.ZMQError as exc:
            logging.warning("Failed to send heartbeat: %s", exc)
    
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
            # Determine poll timeout based on heartbeat schedule
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
            
            # Receive zmq message
            zmq_msg = self.zmq_socket.recv_multipart()

            # received zmq_msg: envelope(client_id) + b"" + body(pfn, pargs)
            envelope, _delimiter_index, body = self._split_envelope(zmq_msg)
            # Logging and validation: body should have at least 2 frames: pfn, pargs(function and arg)
            if len(body) < 2:
                logging.error("Invalid request framing: %s", zmq_msg)
                ret = "Invalid request framing."
                pret = dill.dumps(ret)
                if envelope:
                    self.zmq_socket.send_multipart(envelope + [b""] + [pret])
                else:
                    self.zmq_socket.send(pret)
                continue

            fn = dill.loads(body[0])
            args = dill.loads(body[1])
            fn_name = getattr(fn, "__name__", repr(fn))
            logging.info("Received function: %s ;with %d args", fn_name, len(args))

            try:
                # Execute received func
                ret = fn(*args)
            except Exception as e:
                logging.exception("Function %s raised an exception", fn_name)
                ret = f"An unexpected error occurred: {e}"

            logging.info("Returning from %s -> %s", fn_name, self._summarize_value(ret))

            # Serialize return value
            pret = dill.dumps(ret)

            if envelope:
                self.zmq_socket.send_multipart(envelope + [b""] + [pret])
            else:
                self.zmq_socket.send(pret)

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
            self.zmq_socket.disconnect(self.zmq_addr)
            self.zmq_socket.close()
            self.zmq_context.destroy()