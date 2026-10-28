export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
source ~/DisasterPulse/venv/bin/activate
export HADOOP_HOME=$(brew --prefix hadoop)/libexec
export PATH=$HADOOP_HOME/bin:$PATH
