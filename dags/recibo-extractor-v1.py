from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from minio import Minio
from io import BytesIO
from typing import Dict, List, Any
from contextlib import contextmanager
from langfuse.decorators import observe, langfuse_context

import json
import base64
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id="recibo-extractor-v1",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["recibos", "minio", "llm", "postgres", "v1"],
    doc_md="""
    # Recibo Extractor V1

    Extrai dados estruturados de imagens de recibos (dataset SROIE) usando OpenAI Vision.
    Versão simples: processa um arquivo por vez, sequencialmente.
    """,
)
def recibo_extraction_pipeline():

    @contextmanager
    def get_minio_client():
        conn = BaseHook.get_connection("minio_default")
        extra = json.loads(conn.extra) if conn.extra else {}
        client = Minio(
            endpoint=extra.get("endpoint_url", "minio:9000").replace("http://", ""),
            access_key=conn.login,
            secret_key=conn.password,
            secure=False,
        )
        yield client

    @task()
    def listar_recibos(bucket: str = "recibos", prefix: str = "incoming/") -> List[str]:
        """Lista imagens de recibos pendentes no MinIO."""
        with get_minio_client() as client:
            objects = client.list_objects(bucket, prefix=prefix, recursive=True)
            keys = [
                obj.object_name for obj in objects
                if obj.object_name.lower().endswith((".jpg", ".jpeg", ".png"))
            ]
            logger.info(f"Encontrados {len(keys)} recibos em {bucket}/{prefix}")
            return keys

    @task()
    @observe()
    def extrair_dados(bucket: str, key: str) -> Dict[str, Any]:
        """Baixa a imagem e usa OpenAI Vision para extrair dados estruturados."""
        from openai import OpenAI

        langfuse_context.update_current_trace(
            name="extracao-recibo",
            metadata={"arquivo": key},
        )

        with get_minio_client() as client:
            response = client.get_object(bucket, key)
            image_bytes = response.read()
            response.close()
            response.release_conn()

        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        openai_conn = BaseHook.get_connection("openai_default")
        openai_client = OpenAI(api_key=openai_conn.password)

        prompt = """
        Extraia os seguintes campos deste recibo em formato JSON, sem nenhum texto adicional:
        {
          "empresa": "nome da empresa/estabelecimento",
          "endereco": "endereço completo",
          "data_emissao": "YYYY-MM-DD",
          "total": "valor total, apenas números com ponto decimal"
        }
        """

        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                        },
                    ],
                }
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )

        raw = completion.choices[0].message.content
        data = json.loads(raw)
        data["arquivo_origem"] = key

        langfuse_context.update_current_observation(
        input={"arquivo": key},
        output=data,
        )

        langfuse_context.flush()

        logger.info(f"Extraído de {key}: {data}")
        return data

    @task()
    def validar_e_salvar(dados: Dict[str, Any]):
        """Valida com Pydantic e insere no Postgres."""
        import sys
        sys.path.insert(0, "/opt/airflow/scripts")
        from schema import ReceiptData

        receipt = ReceiptData(
            empresa=dados["empresa"],
            endereco=dados.get("endereco", ""),
            data_emissao=dados["data_emissao"],
            total=dados["total"],
        )

        hook = PostgresHook(postgres_conn_id="recibo_db")
        hook.run(
            """
            INSERT INTO recibos (empresa, endereco, data_emissao, total, arquivo_origem)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (arquivo_origem) DO NOTHING;
            """,
            parameters=(
                receipt.empresa,
                receipt.endereco,
                receipt.data_emissao,
                receipt.total,
                dados["arquivo_origem"],
            ),
        )
        logger.info(f"Salvo no banco: {dados['arquivo_origem']}")

    keys = listar_recibos()
    dados = extrair_dados.partial(bucket="recibos").expand(key=keys)
    validar_e_salvar.expand(dados=dados)


recibo_extraction_pipeline()