# Databricks notebook source
dbutils.fs.ls("dbfs:/databricks/scripts/")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

# COMMAND ----------

script = """
sed -i "s/^exit 101$/exit 0/" /usr/sbin/policy-rc.d
wget https://raw.githubusercontent.com/Microsoft/OMS-Agent-for-Linux/master/installer/scripts/onboard_agent.sh && sh onboard_agent.sh -w b7427e48-abdd-42fc-a65b-e0c8d450f8ba -s CGGguOs/md/zPa/Ae3JZSUXPqmrqP9YusPoLfBmG6F5UF7dB0ROfOmKXWWPSZKZP1ZD/gRH4OO4mdx1w/T3YEg==
sudo su omsagent -c 'python /opt/microsoft/omsconfig/Scripts/PerformRequiredConfigurationChecks.py'
/opt/microsoft/omsagent/bin/service_control restart b7427e48-abdd-42fc-a65b-e0c8d450f8ba
"""

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/configure-omsagent.sh", script, True)