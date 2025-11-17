"""
Spark configuration settings
"""
import os
import subprocess
import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = logging.getLogger(__name__)


def check_java_version():
    """
    Check Java version and warn if incompatible
    
    Returns:
        tuple: (major_version, is_compatible)
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            stderr=subprocess.STDOUT
        )
        # Java version output goes to stderr
        version_output = result.stderr or result.stdout
        
        # Extract version number (e.g., "openjdk version \"24.0.1\"")
        import re
        version_match = re.search(r'version "(\d+)', version_output)
        if version_match:
            major_version = int(version_match.group(1))
            # Spark 3.5 supports Java 8, 11, 17 (and sometimes 21)
            # Java 24 is too new and may cause issues
            is_compatible = major_version in [8, 11, 17, 21]
            
            if not is_compatible:
                logger.warning(
                    f"Java {major_version} detected. Spark 3.5.0 typically supports "
                    "Java 8, 11, 17, or 21. You may encounter compatibility issues. "
                    "Consider using Java 11 or 17 for best compatibility."
                )
            
            return major_version, is_compatible
    except Exception as e:
        logger.warning(f"Could not check Java version: {e}")
    
    return None, True  # Assume compatible if we can't check


def get_spark_session(app_name: str = "CloudProviderAnalytics") -> SparkSession:
    """
    Create and configure Spark session
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    # Check Java version
    java_version, is_compatible = check_java_version()
    
    conf = SparkConf()
    
    # Basic configuration
    conf.set("spark.app.name", app_name)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Java version compatibility settings for newer Java versions
    if java_version and java_version >= 17:
        # For Java 17+, we need to add JVM options to handle removed APIs
        # Specifically fixes: UnsupportedOperationException: getSubject is not supported
        java_opts = (
            "-Dio.netty.tryReflectionSetAccessible=true "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
            # Fix for Subject.getSubject() issue in Java 17+
            # This is critical for Hadoop/Spark compatibility with Java 17+
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/javax.security.auth.login=ALL-UNNAMED "
            "--add-opens=java.base/javax.security.auth.x500=ALL-UNNAMED "
            # Additional security-related opens
            "--add-opens=java.base/java.security=ALL-UNNAMED "
            "--add-opens=java.base/java.security.cert=ALL-UNNAMED "
            "--add-opens=java.base/java.security.acl=ALL-UNNAMED "
        )
        
        conf.set("spark.driver.extraJavaOptions", java_opts)
        conf.set("spark.executor.extraJavaOptions", java_opts)
        
        # Hadoop-specific settings to work around Java 17+ issues
        # Set HADOOP_OPTS environment variable before Spark starts
        hadoop_opts = (
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.security=ALL-UNNAMED "
        )
        if "HADOOP_OPTS" not in os.environ:
            os.environ["HADOOP_OPTS"] = hadoop_opts
    
    # Delta Lake configuration (if using Delta) - commented out by default
    # Uncomment if you install delta-spark
    # conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Performance optimizations
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    conf.set("spark.sql.parquet.mergeSchema", "true")
    
    # Streaming configuration
    conf.set("spark.sql.streaming.checkpointLocation", "datalake/bronze/checkpoints")
    
    # Set environment variable for Hadoop to use Java 17+ compatible settings
    if java_version and java_version >= 17:
        hadoop_opts = (
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.security=ALL-UNNAMED "
        )
        if "HADOOP_OPTS" not in os.environ:
            os.environ["HADOOP_OPTS"] = hadoop_opts
    
    try:
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    except Exception as e:
        error_str = str(e)
        
        # Check for the specific getSubject error
        if "getSubject is not supported" in error_str or "UnsupportedOperationException" in error_str:
            error_msg = (
                f"\n{'='*60}\n"
                f"Java 17+ Compatibility Error: getSubject Issue\n"
                f"{'='*60}\n"
                f"This error occurs because Java 17+ removed Subject.getSubject(),\n"
                f"but Hadoop/Spark still tries to use it.\n\n"
                f"Quick Fix:\n"
                f"1. Set HADOOP_OPTS before running your script:\n"
                f"   export HADOOP_OPTS=\"--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED\"\n\n"
                f"2. Make it permanent (add to ~/.zshrc):\n"
                f"   echo 'export HADOOP_OPTS=\"--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED\"' >> ~/.zshrc\n"
                f"   source ~/.zshrc\n\n"
                f"3. Then try running your script again.\n\n"
                f"Alternative: Use Java 11 instead of Java 17+\n"
                f"   brew install openjdk@11\n"
                f"   export JAVA_HOME=$(/usr/libexec/java_home -v 11)\n"
                f"{'='*60}\n"
            )
            raise RuntimeError(error_msg + f"\nOriginal error: {error_str}")
        
        if java_version and not is_compatible:
            error_msg = (
                f"\n{'='*60}\n"
                f"Java Compatibility Error\n"
                f"{'='*60}\n"
                f"Detected Java {java_version}, but Spark 3.5.0 requires Java 8, 11, 17, or 21.\n\n"
                f"Solutions:\n"
                f"1. Install Java 11 or 17 using Homebrew:\n"
                f"   brew install openjdk@11\n"
                f"   # or\n"
                f"   brew install openjdk@17\n\n"
                f"2. Set JAVA_HOME to use a compatible version:\n"
                f"   export JAVA_HOME=$(/usr/libexec/java_home -v 11)\n"
                f"   # or\n"
                f"   export JAVA_HOME=$(/usr/libexec/java_home -v 17)\n\n"
                f"3. Add to your ~/.zshrc to make it permanent:\n"
                f"   echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc\n"
                f"{'='*60}\n"
            )
            raise RuntimeError(error_msg + f"\nOriginal error: {error_str}")
        raise

