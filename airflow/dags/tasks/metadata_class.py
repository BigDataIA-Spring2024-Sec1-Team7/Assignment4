from pydantic import ( 
    BaseModel,
    field_validator,
    ValidationError,
    ValidationInfo,
)
import re
import validators
from datetime import datetime
import datetime as dt


class MetaDataClass(BaseModel):
    file_size_bytes: int
    num_pages: int
    s3_pypdf_text_link: str | None
    s3_grobid_xml_link:str | None
    file_path: str | None
    encryption: str | None
    date_updated: str | None


    @field_validator('num_pages', 'file_size_bytes')
    @classmethod
    def year_must_be_valid(cls, v: int) -> int:
        if v and not isinstance(v, int) or v <= 0:
            raise ValueError("Invalid number of pages: must be a positive integer.")
        return v
       
    
    @field_validator('date_updated')
    @classmethod
    def date_must_be_valid(cls, v: str) -> str:
        if v and datetime.strptime(v, '%m/%d/%Y').date() > dt.date.today():
            raise ValueError('Invalid date format or date is in the future. Date should be in MM/DD/YYYY format and not in the future.')
        return v


    @field_validator('s3_pypdf_text_link', 's3_grobid_xml_link', 'file_path', 'encryption', 'date_updated')
    @classmethod
    def text_should_not_contain_html_or_quotes(cls, v: str, info: ValidationInfo) -> str:
        if v and re.search('[\'"‘’”“]|<.*?>', v):
            raise ValueError(f'{info.field_name} contains invalid characters like quotes or html tags')
        return v