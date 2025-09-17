from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta


def dag_template(
    dag_id: str,
    image: str,
    cmds: list[str],
    arguments: list[str] = None,
    namespace: str = "default",
    task_id: str = "task",
    env_vars: dict = None,
    default_args: dict = None,
):
    """
    Factory function to create a DAG with KubernetesPodOperator.
    Runs every 5 minutes by default.
    """

    default_args = default_args or {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 1, 1),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval="*/5 * * * *",  # run every 5 minutes
        catchup=False,
    ) as dag:

        k8s_task = KubernetesPodOperator(
            task_id=task_id,
            namespace=namespace,
            image=image,
            cmds=cmds,
            arguments=arguments or [],
            name=f"{dag_id}-pod",
            env_vars=env_vars or {},
            is_delete_operator_pod=True,
            get_logs=True,
        )

        return dag


# Example DAGs created from template

dag1 = dag_template(
    dag_id="k8s_dag_1",
    image="alpine:3.19",
    cmds=["echo"],
    arguments=["Hello from DAG 1"],
)

dag2 = dag_template(
    dag_id="k8s_dag_2",
    image="python:3.10-slim",
    cmds=["python", "-c"],
    arguments=["print('Hello from DAG 2')"],
    env_vars={"ENV": "production"},
)
