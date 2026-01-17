import logging
import os
import zmq
import dill
import dxspaces
import rexec.remote_obj

class RExecServer:
    rexec.remote_obj.DSDataObj.ctx = "server"

    def __init__(self, args):
        self.zmq_addr = "tcp://" + args.broker_addr + ":" + args.broker_port
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.DEALER)

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
    
    def fn_recv_exec(self):
        while(True):
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