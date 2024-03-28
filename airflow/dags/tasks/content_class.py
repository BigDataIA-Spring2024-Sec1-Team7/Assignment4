from pydantic import ( 
    BaseModel,
    field_validator,
    ValidationError,
    ValidationInfo,
)
import re
import validators

class ContentClass(BaseModel):
    level: str 
    title: str 
    topic: str 
    learning_outcomes: str | None

    
    @field_validator('title','learning_outcomes','level')
    @classmethod
    def text_should_not_contain_html_or_quotes(cls, v: str, info: ValidationInfo) -> str:
        if v and re.search('[\'"‘’”“]|<.*?>', v):
            raise ValueError(f'{info.field_name} contains invalid characters like quotes or html tags')
        return v
    
    @field_validator('level')
    @classmethod
    def level_must_match_pattern(cls, v: str) -> str:
        if v and re.search(r"Level\s+(I|II|III)\b", v) == None:
            raise ValueError('level is not valid')
        return v

  