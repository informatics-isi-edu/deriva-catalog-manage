from pydantic import BaseModel, validator, root_validator
from typing import Optional


class MenuOptionList(list):
    def __init__(self, iterable):
        super().__init__(i if isinstance(i, MenuOption) else MenuOption.parse_obj(i) for i in iterable)

    def __setitem__(self, index, value):
        option = value if isinstance(value, MenuOption) else MenuOption.parse_obj(value)
        super().__setitem__(index, option)

    def append(self, *args, **kwargs):
        option = args[0] if isinstance(args[0], MenuOption) else MenuOption.parse_obj(kwargs)
        super().append(option)

    def extend(self, iterable):
        super().extend(MenuOptionList(iterable))


class MenuACL(BaseModel):
    show: Optional[list[str]]
    enable: Optional[list[str]]

    class Config:
        extra = 'forbid'


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

    @validator('children', pre=True)
    def create_option_list(cls, v):
        print(v)
        return v

    @validator('url')
    def chase_url(cls, v):
        # Should check URL to make sure its valid.
        return v

    @root_validator()
    def child_or_url(cls, v):
        if v['children'] and v['url'] or not (v['children'] or v['url']):  # child XOR url
            raise ValueError('Must provide either children or url')
        else:
            return v


class Navbar(BaseModel):
    children: MenuOptionList
    acls: Optional[MenuACL]
    newTab: Optional[bool]

    class Config:
        extra = 'forbid'

    @validator('children', pre=True)
    def create_option_list(cls, v):
        print(v)
        return MenuOptionList(v)


class NavbarMenu(BaseModel):
    navbarMenu: Navbar

    class Config:
        extra = 'forbid'


navspec_test = {
    'navbarMenu': {
        'newTab': False,
        'children': [
            {"name": "Search", "children": [
                {"name": "Gene Expression Data", "children": [
                    {"name": "Genes", "url": "/chaise/recordset/#2/Common:Gene"},
                    {"name": "Sequencing Data (GUDMAP pre-2018)", "children": [
                        {"name": "Series", "url": "/chaise/recordset/#2/Legacy_RNASeq:Series"},
                        {"name": "Samples", "url": "/chaise/recordset/#2/Legacy_RNASeq:Sample"},
                        {"name": "Protocols", "url": "/chaise/recordset/#2/Legacy_RNASeq:Protocol"}
                    ]},
                    {"name": "Specimens", "url": "/chaise/recordset/#2/Gene_Expression:Specimen"}
                ]},
                {"name": "Cell & Animal Models", "children": [
                    {"name": "Parental Cell Lines", "url": "/chaise/recordset/#2/Cell_Line:Parental_Cell_Line"},
                    {"name": "Mouse Strains", "url": "/chaise/recordset/#2/Cell_Line:Mouse_Strain"}
                ]}
            ]},
            {"name": "Create", "children": [
                {"name": "Protocol", "children": [
                    {"name": "Protocol", "url": "/chaise/recordedit/#2/Protocol:Protocol"},
                    {"name": "Subject", "url": "/chaise/recordedit/#2/Protocol:Subject"},
                    {"name": "Keyword", "url": "/chaise/recordedit/#{{{$catalog.id}}}/Vocabulary:Keyword"}
                ]}
            ]},
            {"name": "Help", "children": [
                {"name": "Using the Data Browser",
                 "url": "https://github.com/informatics-isi-edu/gudmap-rbk/wiki/Using-the-GUDMAP-RBK-Data-Browser"},
                {"name": "Submitting Data", "url": "https://github.com/informatics-isi-edu/gudmap-rbk/wiki"},
                {"name": "Create Citable Datasets",
                 "url": "https://github.com/informatics-isi-edu/gudmap-rbk/wiki/Create-citable-datasets"},
                {"name": "Cite Consortium Data", "url": "/about/usage.html"}
            ]}
        ]
    }
}
