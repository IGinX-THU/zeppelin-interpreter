# LEDD Example

Ensure a stable network connection, and use a proxy if necessary.

## build

Build the **zeppelin-interpreter** and **schemapile-loader**:

```shell
mvn -f ../../pom.xml clean package
mvn -f schemapile-loader/pom.xml clean package
```

Build and run the containers:

```shell
docker compose up --build
```

## Usage

After the IGinX container starts, wait for the IGinX-init container to complete initialization.

To avoid potential bugs with Milvus, restart the Milvus container once the above steps are completed.

Open http://localhost:8080 to access the Zeppelin page. Open the example notebook and execute the sample statements. Some statements may take a long time to execute; you can check the progress through the IGinX logs.
