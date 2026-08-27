from pydantic import BaseModel, Field
from datetime import date
from decimal import Decimal


class ReceiptData(BaseModel):
    empresa: str
    endereco: str
    data_emissao: date
    total: Decimal