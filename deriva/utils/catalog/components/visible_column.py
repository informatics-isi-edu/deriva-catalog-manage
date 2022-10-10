from __future__ import annotations

from pydantic import BaseModel as PydanticBaseModel
from pydantic import validator, root_validator, Field, Extra, conlist
from typing import Optional, Union, Generic, TypeVar, Literal, Any
import deriva.core.ermrest_model as em

Current_Model: Optional[em.Model] = None


class BaseModel(PydanticBaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True
        catalog_model = Current_Model


ElementType = TypeVar('ElementType')


class TypedList(Generic[ElementType], list):
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
        print(f"TypedList {v}")
        return cls([ElementType.parse_obj(e) for e in v])

    def __setitem__(self, index, value):
        super()[index] = value if type(value) == ElementType else ElementType.parse_obj(value)

    def append(self, obj):
        super().append(obj if type(obj) == ElementType else ElementType.parse_obj(obj))

    def extend(self, iterable):
        super().extend(ElementType(iterable))


class Column:
    pass


class Fkey:
    pass


class ColumnOrder:
    pass


class ArrayOptions(BaseModel):
    order: Any  # Optional[ColumnOrder]
    max_length: Any  # Optional[int]


class DisplayOptions(BaseModel):
    markdown_pattern: str
    template_engine: str
    wait_for: TypedList[ColumnDirective]
    show_foreign_key_link: Optional[bool]
    show_key_link: Optional[bool]
    array_ux_mode: Literal["olist", "ulist", "csv", "raw"]
    array_options: ArrayOptions


class ColumnSourceSpec(BaseModel):
    inbound: Optional[list]
    outbound: Optional[list]
    sourcekey: Optional[str]

    @validator('inbound', 'outbound')
    def validate_spec(cls, v):
        if len(v) != 2:
            ValueError("inbound/outbout value must be list of length 2")
        return v


class SourcePathSourceKeyElement(BaseModel):
    sourcekey: str


class SourcePathDirectionElement(BaseModel):
    inbound: Optional[conlist(str, min_items=2, max_items=2)]
    outbound: Optional[conlist(str, min_items=2, max_items=2)]

    @root_validator()
    def validate_direction(cls, val):
        if val.get('inbound') and val.get('outbound') or not (val.get('inbound') or val.get('outbound')):
            raise ValueError('Must specify inbound or outboud')
        return val


class SourceBoolElement(BaseModel):
    source_and: list[Any] = Field(alias="and")
    source_or: list[Any] = Field(alias="or")
    negate: Optional[bool]


FilterOperators = Literal[
    '::null:',
    '=',
    '::lt::',
    '::leq::',
    '::gt::',
    '::geq::',
    '::regexp::',
    '::ciregexp::',
    '::ts::'
]


class SourceFilterElement(BaseModel):
    filter: str
    operator: Optional[FilterOperators]
    negate: Optional[bool]


SourcePathElement = Union[
    str,
    SourcePathSourceKeyElement,
    SourcePathDirectionElement,
    SourceBoolElement,
    SourceFilterElement
]

class ColumnDirective(BaseModel):
    source: Optional[conlist(SourcePathElement)]
    sourcekey: Optional[str]
    entity: Optional[bool]
    aggregate: Any  # This should be restricted to only visible_columns specification.
    markdown_name: Optional[str]
    comment: Optional[str]
    comment_display: Optional[Literal["tooltip", "inline"]]
    hide_column_header: Optional[bool]
    self_link: Optional[bool]
    display: Optional[DisplayOptions]
    array_options: Any

    @validator('source', pre=True)
    def make_source_list(cls, v):
        # Normalize {'source': colname} into {'source': [colname]}
        return [v] if isinstance(v, str) else v

    @root_validator()
    def without_source(cls, v):
        # Handle case column directive without any source
        if not (v.get('source') or v.get('sourcekey') or
            (v.get('display') and v.get('display').get("markdown_patterh"))):
                ValueError("Must have either source/sourckey or markdown-name and display/markdown_pattern")
        else:
            return v


class SearchBoxSource(BaseModel):
    source: str
    markdown_name: Optional[str]


class SearchBox(BaseModel):
    search_or: TypedList[SearchBoxSource] = Field(alias="or")


class SourceDefinition(BaseModel):
    sources: dict[str, ColumnDirective]
    search_box: SearchBox = Field(alias="search-box")
    fkeys: Union[TypedList[ColumnDirective], bool]
    columns: Union[conlist(str), bool]

    @validator('columns')
    def validate_columns(cls, v):
        if not v:
            ValueError("Column value must be a columnlist or True")
        return v

    @validator('fkeys')
    def validate_columns(cls, v):
        if not v:
            ValueError("Fkeys value must be a fkey list or True")
        return v


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
