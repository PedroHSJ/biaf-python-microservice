
from datetime import datetime
from typing import Optional
import math
class LegalPersonDto:
    def __init__(self):
        self.cnpj: Optional[str] = None
        self.cnpj_base: Optional[str] = None
        self.branch_matrix_number: Optional[int] = None
        self.trade_name: Optional[str] = None
        self.legal_name: Optional[str] = None
        self.registration_status_number: Optional[int] = None
        self.registration_status_date: Optional[str] = None
        self.reason_number_registration_status: Optional[int] = None
        self.name_outer_township: Optional[str] = None
        self.country_number: Optional[int] = None
        self.date_start_activity: Optional[str] = None
        self.number_cnae_primary: Optional[int] = None
        self.description_cnae_secondary: Optional[str] = None
        self.description_type_street: Optional[str] = None
        self.address: Optional[str] = None
        self.street_number: Optional[str] = None
        self.ds_complemento: Optional[str] = None
        self.neighborhood: Optional[str] = None
        self.postal_code: Optional[str] = None
        self.uf: Optional[str] = None
        self.township_number: Optional[int] = None
        self.ddd1: Optional[int] = None
        self.phone_number1: Optional[str] = None
        self.ddd2: Optional[int] = None
        self.phone_number2: Optional[str] = None
        self.ddd_fax: Optional[int] = None
        self.fax_number: Optional[str] = None
        self.email: Optional[str] = None
        self.description_special_situation: Optional[str] = None
        self.date_special_situation: Optional[str] = None
        self.id: Optional[str] = None
        self.dt_inclusao: Optional[datetime] = None
        self.dt_alteracao: Optional[datetime] = None

    def __repr__(self):
        return f"<LegalPersonDto cnpj={self.cnpj}, trade_name={self.trade_name}>"

def parse_int_safe(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def truncate_and_parse_int(value):
    try:
        return int(value[:2])
    except (ValueError, TypeError):
        return None



def transform_row_to_entity(row, down_file, pj=None):
    try:
        def safe_value(value):
            return None if isinstance(value, float) and math.isnan(value) else value
        
        if down_file == 'Estabelecimentos':
            legal_person = LegalPersonDto()
            legal_person.id = row[0] + row[1] + row[2]
            legal_person.cnpj = row[0] + row[1] + row[2]
            legal_person.cnpj_base = row[0]
            legal_person.branch_matrix_number = safe_value(parse_int_safe(row[3]))
            legal_person.trade_name = safe_value(row[4])
            legal_person.legal_name = None
            legal_person.registration_status_number = safe_value(parse_int_safe(row[5]))
            legal_person.registration_status_date = safe_value(row[6])
            legal_person.reason_number_registration_status = safe_value(parse_int_safe(row[7]))
            legal_person.name_outer_township = safe_value(row[8])
            legal_person.country_number = safe_value(parse_int_safe(row[9]))
            legal_person.date_start_activity = safe_value(row[10])
            legal_person.number_cnae_primary = safe_value(parse_int_safe(row[11]))
            legal_person.description_cnae_secondary = safe_value(row[12])
            legal_person.description_type_street = safe_value(row[13])
            legal_person.address = safe_value(row[14])
            legal_person.street_number = safe_value(row[15])
            legal_person.ds_complemento = safe_value(row[16])
            legal_person.neighborhood = safe_value(row[17])
            legal_person.postal_code = safe_value(row[18])
            legal_person.uf = row[19][:2] if len(row[19]) > 2 else row[19]
            legal_person.township_number = safe_value(parse_int_safe(row[20]))
            legal_person.ddd1 = safe_value(truncate_and_parse_int(row[21]))
            legal_person.phone_number1 = safe_value(row[22])
            legal_person.ddd2 = safe_value(truncate_and_parse_int(row[23]))
            legal_person.phone_number2 = safe_value(row[24])
            legal_person.ddd_fax = safe_value(truncate_and_parse_int(row[25]))
            legal_person.fax_number = safe_value(row[26])
            legal_person.email = safe_value(row[27])
            legal_person.description_special_situation = safe_value(row[28])
            legal_person.date_special_situation = safe_value(row[29])

            if pj:
                legal_person.id = pj.get('id')
                legal_person.dt_alteracao = datetime.now()
            else:
                legal_person.dt_inclusao = datetime.now()

            return legal_person

        if down_file == 'Empresas' and pj:
            legal_person = LegalPersonDto()
            legal_person.__dict__.update(pj)
            legal_person.legal_name = safe_value(row[1])
            legal_person.dt_alteracao = datetime.now()
            return legal_person

        if down_file == 'Empresas' and not pj:
            return None

        return None
    except Exception as e:
        print(f"Error transforming row to entity: {e}")

