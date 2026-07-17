# Tiny stdlib-only agent image.
FROM python:3.12-slim
WORKDIR /app
COPY edge_agent.py /app/edge_agent.py
# No third-party deps on purpose — keeps the appliance surface small.
USER 1000:1000
ENTRYPOINT ["python", "-u", "/app/edge_agent.py"]
