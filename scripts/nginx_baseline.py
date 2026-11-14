"""Owned foreground NGINX instance with no generation retries or buffering."""
import contextlib
import hashlib
import json
from pathlib import Path

from check_real_pool import service


@contextlib.contextmanager
def nginx_router(binary, condition, origins, output, label, commands):
    binary=Path(binary).resolve();prefix=(output/(label+"-nginx")).resolve()
    prefix.mkdir(parents=True,exist_ok=True);(prefix/"logs").mkdir(exist_ok=True);(prefix/"temp").mkdir(exist_ok=True)
    method="hash $http_x_prefix_key consistent;" if condition=="nginx-hash" else ""
    servers="\n".join("server "+origin.removeprefix("http://")+" max_conns=1 max_fails=0;" for origin in origins)
    configuration=f"""worker_processes 1;
events {{ worker_connections 128; }}
http {{
    access_log off;
    upstream engines {{ {method} {servers} }}
    server {{
        listen 127.0.0.1:18080;
        client_max_body_size 64k;
        client_body_timeout 10s;
        location = /readyz {{ return 200 'ready'; }}
        location /v1/chat/completions {{
            proxy_pass http://engines;
            proxy_http_version 1.1;
            proxy_set_header Connection '';
            proxy_buffering off;
            proxy_request_buffering on;
            proxy_next_upstream off;
            proxy_read_timeout 60s;
            proxy_send_timeout 60s;
            error_page 502 503 =503 @busy;
        }}
        location @busy {{ add_header Retry-After 1 always; return 503; }}
    }}
}}
"""
    path=prefix/"nginx.conf";path.write_text(configuration,encoding="utf-8")
    command=[str(binary),"-p",prefix.as_posix()+"/","-c",path.as_posix(),"-g","daemon off; master_process off;"]
    commands.append(command)
    metadata={"binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),"configuration":configuration,
              "scope":"general HTTP router; upstream connection bound one per engine; no retries; one foreground process"}
    with service(command,"http://127.0.0.1:18080/readyz",output/(label+"-nginx.log")):
        yield metadata
