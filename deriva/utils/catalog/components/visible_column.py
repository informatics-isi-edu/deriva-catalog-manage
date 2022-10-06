from pydantic import BaseModel, validator, root_validator
from typing import Optional, Union
from deriva.core.ermrest_model import Model


class MenuACL(BaseModel):
    show: Optional[list[str]]
    enable: Optional[list[str]]

    class Config:
        extra = 'forbid'


class ColumnList(list):
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
        super().extend(ColumnList(iterable))


class MenuOption(BaseModel):
    name: str
    markdownName: Optional[str]
    url: Optional[str]
    children: Optional[MenuOptionList]
    acls: Optional[MenuACL]
    header: Optional[bool]
    newTab: Optional[bool]

    class Config:
        extra = 'forbid'

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

class SourceDefinitions:
    pass

class FkeyList:
    pass

class Column:
    pass

class SearchBox(BaseModel):
    or: ColumnList

    class Config:
    extra = 'forbid'


class Source(BaseModel):
    sources: SourceDefinitions
    search_box: SearchBox
    fkeys: Union[FkeyList, bool]
    columns: Union[ColumnList, bool]

    class Config:
        extra = 'forbid'

    @validator('columns')
    def validate_columns(cls, v):
        if not v:
            ValueError("Column value must be a columnlist or True")

    @validator('fkeys')
    def validate_columns(cls, v):
        if not v:
            ValueError("Fkeys value must be a fkey list or True")

example = {
    "columns": True,
    "fkeys": True,
    "sources": {
        "source-1": {
            "source": [{"inbound": ["schema", "fk1"]}, "RID"],
            "entity": True,
            "aggregate": "array_d"
        },
        "source-2": {
            "source": "column",
            "markdown_name": "Column displayname"
        },
        "source-3": {
            "source": [
                {"sourcekey": "source-1"},
                {"outbound": ["schema", "fk2"]},
                "RID"
            ]
        }
    },
    "search-box": {
        "or": [
            {"source": "column1", "markdown_name": "another name"},
            {"source": "column2"},
            {"source": "column3", "markdown_name": "Column 3 name"},
        ]
    }
}