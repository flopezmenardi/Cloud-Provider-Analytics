#!/bin/bash
# Setup script para Cloud Provider Analytics

# Obtener directorio del script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activar entorno virtual
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Creando entorno virtual..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
fi

# Configurar Java 17 (requerido por PySpark 4.x)
if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
elif [ -d "/usr/local/opt/openjdk@17" ]; then
    export JAVA_HOME="/usr/local/opt/openjdk@17"
else
    export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null)
fi

if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    echo "ERROR: Java 17 no encontrado."
    echo "Instala con: brew install openjdk@17"
    return 1
fi

export PATH=$JAVA_HOME/bin:$PATH

# CRITICAL: Fix para Java 17+ con Spark/Hadoop (getSubject issue)
# Estas opciones DEBEN setearse ANTES de que Python importe PySpark
JAVA_17_OPTS="--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false"

export _JAVA_OPTIONS="$JAVA_17_OPTS"
export HADOOP_OPTS="$JAVA_17_OPTS"
export SPARK_SUBMIT_OPTS="$JAVA_17_OPTS"

echo ""
echo "======================================"
echo "Cloud Provider Analytics - Entorno OK"
echo "======================================"
echo "  Python: $(python --version 2>&1)"
echo "  Java:   $(java -version 2>&1 | head -n 1)"
echo "  JAVA_HOME: $JAVA_HOME"
echo "  venv:   activado"
echo "  Java 17 fix: aplicado"
echo ""
