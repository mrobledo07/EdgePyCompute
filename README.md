# 🚀 PyEdgeCompute

**PyEdgeCompute** is a framework that enables users to run parallel scientific Python applications across personal and edge devices (e.g., laptops, Raspberry Pis), rather than relying on expensive cloud or proprietary clusters.

By compiling Python code to WebAssembly (via Pyodide), the system allows seamless and dependency-free execution across any machine with a browser or Node.js runtime. This means no installations, no dependency hell — just fast, portable scientific computation.

🔬 Designed for scientists and data analysts using Python — the most widely adopted language in the field — **PyEdgeCompute** makes distributed computing simple and accessible, even on constrained edge devices.

💡 Inspired by the Wasimoff project, but extended to support more complex workloads and external data I/O.

---

## 🐳 How to execute the project

1. Install Docker and Docker Compose
   - [Docker](https://docs.docker.com/get-docker/)
   - [Docker Compose](https://docs.docker.com/compose/install/)
2. Clone the repository
3. Execute the following commands:

3.1. This command will build the Docker images for the orchestrator and the minio object storage.:
```bash
cd orchestrator/
sudo docker-compose up
```

3.2. This command will build the Docker image for the client and start the client object storage:
```bash
cd client/
sudo docker-compose up
```

4. Execute test.sh
```bash
bash test_mapreduce.sh
```

Maybe you would want to execute test_mapreduce.sh before the client docker-compose up so you have all the workers available to execute your mapreduce job.
