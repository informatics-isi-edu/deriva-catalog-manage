from pydantic import validator, root_validator, Extra
from pydantic import BaseModel as PydanticBaseModel
from typing import Optional
from deriva.core.ermrest_model import Model


class BaseModel(PydanticBaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class MenuACL(BaseModel):
    show: Optional[list[str]]
    enable: Optional[list[str]]

class MenuOptionList(list):
    """.
    """

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return cls([MenuOption.parse_obj(e) for e in v])

    def __setitem__(self, index, value):
        super()[index] = value if isinstance(value, MenuOption) else MenuOption.parse_obj(value)

    def append(self, obj):
        super().append(obj if isinstance(obj, MenuOption) else MenuOption.parse_obj(obj))

    def extend(self, iterable):
        super().extend(MenuOptionList(iterable))


class MenuOption(BaseModel):
    name: str
    markdownName: Optional[str]
    url: Optional[str]
    children: Optional[MenuOptionList]
    acls: Optional[MenuACL]
    header: Optional[bool]
    newTab: Optional[bool]

    @classmethod
    def menu_url(cls, schema_name, table_name):
        return MenuOption(name=table_name,
                          url="/chaise/recordset/#{{{$catalog.id}}}/" + "{}:{}".format(schema_name, table_name))

    @validator('url')
    def chase_url(cls, v):
        # Should check URL to make sure its valid.
        return v

    @root_validator()
    def child_or_url(cls, v):
        if (v.get('children') and v.get('url')) or not (v.get('children') or v.get('url')):  # child XOR url
            raise ValueError('Must provide either children or url')
        else:
            return v


class NavbarMenu(BaseModel):
    children: MenuOptionList
    acls: Optional[MenuACL]
    newTab: Optional[bool]

    @staticmethod
    def get_navbar(model: Model):
        return NavbarMenu.parse_obj(model.annotations['tag:isrd.isi.edu,2019:chaise-config']['navbarMenu'])

    def set_navbar(self, model: Model):
        model.annotations['tag:isrd.isi.edu,2019:chaise-config']['navbarMenu'] = self.dict(exclude_none=True)
