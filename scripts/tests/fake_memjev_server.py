#!/usr/bin/env python3
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port_file, request_file = sys.argv[1:]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        with open(request_file, "a", encoding="utf-8") as output:
            output.write(json.dumps({
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "idempotency_key": self.headers.get("Idempotency-Key"),
                "body": body,
            }, separators=(",", ":")) + "\n")

        if self.path.endswith("/Retrieve"):
            response = {
                "retrievalRunId": "retrieval_demo",
                "disposition": "RETRIEVAL_DISPOSITION_SELECTED",
                "plan": {
                    "injectionId": "injection_demo",
                    "taskExecutionId": "execution_demo",
                    "nodes": [{"ordinal": 1, "procedureVersionId": "procedure_demo"}],
                },
            }
        elif self.path.endswith("/IngestTrace"):
            response = {
                "receiptId": "receipt_demo",
                "traceId": "trace_demo",
                "disposition": "INGEST_DISPOSITION_ACCEPTED",
                "acceptedEvents": 1,
            }
        elif self.path.endswith("/RecordOutcome"):
            response = {
                "outcomeId": "outcome_demo",
                "traceId": "trace_demo",
                "state": "OUTCOME_STATE_VERIFIED_SUCCESS",
                "disposition": "OUTCOME_DISPOSITION_ACCEPTED",
            }
        else:
            self.send_error(404)
            return

        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, _format, *_args):
        return


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w", encoding="utf-8") as output:
    output.write(str(server.server_port))
server.serve_forever()
