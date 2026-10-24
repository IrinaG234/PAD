"""Regenerate protobuf messages and gRPC client/server stubs."""
from pathlib import Path
from grpc_tools import protoc
import grpc_tools

if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    includes = Path(grpc_tools.__file__).parent / '_proto'
    result = protoc.main(['protoc', '-I'+str(root), '-I'+str(includes),
                          '--python_out='+str(root), '--grpc_python_out='+str(root),
                          str(root/'broker.proto')])
    raise SystemExit(result)
