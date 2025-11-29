#!/bin/bash
# Setup script para Cloud Provider Analytics

# Activar entorno virtual
source venv/bin/activate

# Configurar Java 17 (requerido por PySpark 3.5+)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH

echo "✓ Entorno configurado"
echo "  Python venv: activado"
echo "  Java version:"
java -version 2>&1 | head -n 1
