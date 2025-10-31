# Fugue

This is the implementation of the Fugue system described in the paper "Fugue: Online and Fine-Grained Elasticity for Distributed Stateful Stream Processing".

## Quickstart

### Prerequisites
- Java 11 or higher
- Maven 3.6+
- Apache Flink 1.18

### Build and run Fugue with Flink

```bash
# Clone Flink 1.18
git clone https://github.com/apache/flink.git
cd flink
git checkout release-1.18

# Initial build
mvn clean install -DskipTests

# Navigate to Fugue directory
cd /path/to/fugue-flink

# Run deployment script
./deploy-to-flink.sh /path/to/flink

cd /path/to/flink

# Build Flink with Fugue integration
./path/to/fugue-flink/deploy-to-flink.sh /path/to/flink --build

# Start cluster
cd /path/to/flink
./bin/start-cluster.sh

# Submit example job with Fugue
./bin/flink run \
    -c org.apache.flink.fugue.examples.WordCountWithFugue \
    /path/to/fugue-examples.jar
```
