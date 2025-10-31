#!/bin/bash

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FUGUE_DIR="$SCRIPT_DIR"

# Flink source directory (from argument or default)
FLINK_DIR="${1:-}"

# Backup directory
BACKUP_DIR="$FLINK_DIR/fugue-backup-$(date +%Y%m%d-%H%M%S)"


log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

validate_prerequisites() {
    log_info "Validating prerequisites..."

    check_command java
    check_command mvn
    check_command git

    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -lt 11 ]; then
        log_error "Java 11 or higher is required. Found: Java $JAVA_VERSION"
        exit 1
    fi
    log_info "Java version: OK"

    MVN_VERSION=$(mvn -version | head -n 1 | awk '{print $3}')
    log_info "Maven version: $MVN_VERSION"

    if [ -z "$FLINK_DIR" ]; then
        log_error "Usage: $0 <path-to-flink-source>"
        exit 1
    fi

    if [ ! -d "$FLINK_DIR" ]; then
        log_error "Flink directory not found: $FLINK_DIR"
        exit 1
    fi

    if [ ! -f "$FLINK_DIR/pom.xml" ]; then
        log_error "Not a valid Flink source directory: $FLINK_DIR"
        exit 1
    fi

    log_info "Prerequisites validated successfully"
}

create_backup() {
    log_info "Creating backup of Flink source files..."

    mkdir -p "$BACKUP_DIR"

    # Backup files that will be modified
    FILES_TO_BACKUP=(
        "flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java"
        "flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskExecutor.java"
        "flink-runtime/src/main/java/org/apache/flink/runtime/io/network/api/serialization/EventSerializer.java"
        "flink-runtime/pom.xml"
    )

    for file in "${FILES_TO_BACKUP[@]}"; do
        if [ -f "$FLINK_DIR/$file" ]; then
            mkdir -p "$BACKUP_DIR/$(dirname $file)"
            cp "$FLINK_DIR/$file" "$BACKUP_DIR/$file"
            log_info "Backed up: $file"
        fi
    done

    log_info "Backup created at: $BACKUP_DIR"
}

copy_fugue_modules() {
    log_info "Copying Fugue modules into Flink..."

    FLINK_FUGUE_DIR="$FLINK_DIR/flink-runtime/src/main/java/org/apache/flink/fugue"

    mkdir -p "$FLINK_FUGUE_DIR"

    log_info "Copying fugue-common..."
    cp -r "$FUGUE_DIR/fugue-common/src/main/java/org/apache/flink/fugue/common" "$FLINK_FUGUE_DIR/"

    log_info "Copying fugue-coordinator..."
    cp -r "$FUGUE_DIR/fugue-coordinator/src/main/java/org/apache/flink/fugue/coordinator" "$FLINK_FUGUE_DIR/"

    log_info "Copying fugue-runtime..."
    cp -r "$FUGUE_DIR/fugue-runtime/src/main/java/org/apache/flink/fugue/runtime" "$FLINK_FUGUE_DIR/"

    log_info "Copying fugue-integration..."
    cp -r "$FUGUE_DIR/fugue-integration/src/main/java/org/apache/flink/fugue/integration" "$FLINK_FUGUE_DIR/"

    log_info "Fugue modules copied successfully"
}

update_dependencies() {
    log_info "Updating Flink dependencies..."

    RUNTIME_POM="$FLINK_DIR/flink-runtime/pom.xml"

    # Check if RocksDB dependency already exists
    if grep -q "rocksdbjni" "$RUNTIME_POM"; then
        log_warn "RocksDB dependency already present in pom.xml"
    else
        # Add RocksDB dependency
        log_info "Adding RocksDB dependency to flink-runtime/pom.xml..."

        # Find the dependencies section and add RocksDB
        # This is a simplified approach - may need adjustment
        sed -i.bak '/<\/dependencies>/i\
        <dependency>\
            <groupId>org.rocksdb</groupId>\
            <artifactId>rocksdbjni</artifactId>\
            <version>7.9.2</version>\
        </dependency>' "$RUNTIME_POM"

        log_info "RocksDB dependency added"
    fi
}

generate_patches() {
    log_info "Generating integration patch files..."

    PATCH_DIR="$FUGUE_DIR/patches"
    mkdir -p "$PATCH_DIR"

    # Generate JobMaster patch instructions
    cat > "$PATCH_DIR/JobMaster.patch.txt" << 'EOF'
# JobMaster Integration Instructions
# File: flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java

## Add imports:
import org.apache.flink.fugue.integration.jobmanager.FugueJobMasterService;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;

## Add field to JobMaster class:
private FugueJobMasterService fugueService;

## In start() method, after super.start():
// Initialize Fugue service
if (jobGraph.getJobConfiguration().getBoolean("fugue.elasticity.enabled", false)) {
    PolicyConfiguration fugueConfig = new PolicyConfiguration();
    // Load configuration from job config
    this.fugueService = new FugueJobMasterService(
        jobGraph.getJobID(),
        getRpcService(),
        fugueConfig);
    fugueService.start();
}

## In onStop() method, before super.onStop():
// Stop Fugue service
if (fugueService != null) {
    fugueService.stop();
}
EOF

    # Generate TaskExecutor patch instructions
    cat > "$PATCH_DIR/TaskExecutor.patch.txt" << 'EOF'
# TaskExecutor Integration Instructions
# File: flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskExecutor.java

## Add imports:
import org.apache.flink.fugue.integration.taskmanager.FugueTaskExecutorService;

## Add field to TaskExecutor class:
private FugueTaskExecutorService fugueService;

## In start() method, after super.start():
// Initialize Fugue service
String taskManagerLocation = getAddress() + ":" + getPort();
this.fugueService = new FugueTaskExecutorService(
    getRpcService(),
    taskManagerLocation);
fugueService.start();

## In onStop() method:
if (fugueService != null) {
    return fugueService.onStop().thenCompose(v -> super.onStop());
}
return super.onStop();
EOF

    log_info "Patch files generated in: $PATCH_DIR"
    log_warn "Manual patching required - see patch files for instructions"
}

build_flink() {
    log_info "Building Flink with Fugue integration..."

    cd "$FLINK_DIR"

    # Clean previous builds
    log_info "Cleaning previous builds..."
    mvn clean

    # Build Flink (skip tests for faster build)
    log_info "Building Flink (this may take 10-15 minutes)..."
    if mvn install -DskipTests -T 4; then
        log_info "Build successful!"
    else
        log_error "Build failed. Check the output above for errors."
        exit 1
    fi
}


verify_integration() {
    log_info "Verifying integration..."

    # Check if Fugue classes are in the build
    FLINK_DIST_JAR=$(find "$FLINK_DIR/flink-dist/target" -name "flink-dist*.jar" | head -n 1)

    if [ -z "$FLINK_DIST_JAR" ]; then
        log_error "flink-dist jar not found"
        return 1
    fi

    log_info "Checking for Fugue classes in: $FLINK_DIST_JAR"

    # Check for key Fugue classes
    if jar tf "$FLINK_DIST_JAR" | grep -q "org/apache/flink/fugue"; then
        log_info "✓ Fugue classes found in distribution"
    else
        log_warn "✗ Fugue classes not found in distribution"
        log_warn "This is expected if using separate JARs"
    fi

    # Check flink-runtime jar
    RUNTIME_JAR=$(find "$FLINK_DIR/flink-runtime/target" -name "flink-runtime*.jar" | head -n 1)
    if [ -n "$RUNTIME_JAR" ]; then
        if jar tf "$RUNTIME_JAR" | grep -q "org/apache/flink/fugue"; then
            log_info "✓ Fugue classes found in flink-runtime"
        else
            log_error "✗ Fugue classes not found in flink-runtime"
            return 1
        fi
    fi

    log_info "Verification completed"
}

rollback() {
    log_warn "Rolling back changes..."

    if [ ! -d "$BACKUP_DIR" ]; then
        log_error "Backup directory not found: $BACKUP_DIR"
        return 1
    fi

    # Restore backed up files
    cp -r "$BACKUP_DIR"/* "$FLINK_DIR/"

    # Remove Fugue modules
    rm -rf "$FLINK_DIR/flink-runtime/src/main/java/org/apache/flink/fugue"

    log_info "Rollback completed. Original files restored from: $BACKUP_DIR"
}

main() {
    log_info "Starting Fugue-Flink integration deployment"
    log_info "Fugue directory: $FUGUE_DIR"
    log_info "Flink directory: $FLINK_DIR"

    validate_prerequisites

    create_backup

    copy_fugue_modules

    update_dependencies

    generate_patches

    log_info ""
    log_info "====================== MANUAL STEPS REQUIRED ======================"
    log_warn "Fugue modules have been copied, but source files need manual patching."
    log_warn "Please apply the patches in: $FUGUE_DIR/patches/"
    log_warn ""
    log_warn "After applying patches, run this script with --build flag:"
    log_warn "  $0 $FLINK_DIR --build"
    log_info "==================================================================="
    log_info ""

    # If --build flag provided, build
    if [ "$2" == "--build" ]; then
        log_info "Building Flink with Fugue integration..."
        build_flink
        verify_integration

        log_info ""
        log_info "====================== INTEGRATION COMPLETE ======================"
        log_info "Fugue has been integrated with Flink!"
        log_info ""
        log_info "Next steps:"
        log_info "1. Start Flink cluster: $FLINK_DIR/bin/start-cluster.sh"
        log_info "2. Submit a Fugue-enabled job"
        log_info "3. Monitor logs: tail -f $FLINK_DIR/log/flink-*-jobmanager-*.log"
        log_info ""
        log_info "To rollback: $0 $FLINK_DIR --rollback"
        log_info "==================================================================="
    fi

    # If --rollback flag provided, rollback
    if [ "$2" == "--rollback" ]; then
        rollback
    fi
}

# Trap errors and provide helpful message
trap 'log_error "An error occurred. Check the output above for details. To rollback, run: $0 $FLINK_DIR --rollback"' ERR

main "$@"