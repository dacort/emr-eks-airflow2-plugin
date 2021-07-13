from emr_containers.sensors.emr_containers import EMRContainerSensor
from emr_containers.operators.emr_containers import EMRContainerOperator
from emr_containers.hooks.emr_containers import EMRContainerHook
from airflow.plugins_manager import AirflowPlugin
                    
class EMRContainerPlugin(AirflowPlugin):
                    
    name = 'emr_containers_plugin'
                    
    hooks = [EMRContainerHook]
    operators = [EMRContainerOperator]
    sensors = [EMRContainerSensor]
