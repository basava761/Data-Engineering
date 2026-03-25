from airflow.plugins_manager import AirflowPlugin from airflow.models.baseoperator import BaseOperator from 
airflow.utils.decorators import apply_defaults
# A very simple custom operator
class HelloOperator(BaseOperator): @apply_defaults def __init__(self, name: str, **kwargs): super().__init__(**kwargs) self.name 
        = name
    def execute(self, context): print(f"Hello from {self.name}!")
# Register the plugin
class MyPlugin(AirflowPlugin): name = "my_plugin"
    operators = [HelloOperator]
