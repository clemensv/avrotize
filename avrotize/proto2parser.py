#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# adapted from: https://github.com/xophiix/proto2parser/

from lark import Lark, Transformer, Tree, Token
from collections import namedtuple
import typing
import json

BNF = r'''
OCTALDIGIT: "0..7"
IDENT: ( "_" )* LETTER ( LETTER | DECIMALDIGIT | "_" )*
FULLIDENT: IDENT ( "." IDENT )*
MESSAGENAME: IDENT
ENUMNAME: IDENT
FIELDNAME: IDENT
ONEOFNAME: IDENT
MAPNAME: IDENT
SERVICENAME: IDENT
TAGNAME: IDENT
TAGVALUE: IDENT
RPCNAME: IDENT
MESSAGETYPE: [ "." ] ( IDENT "." )* MESSAGENAME
ENUMTYPE: [ "." ] ( IDENT "." )* ENUMNAME
CAPITALLETTER: "A..Z"
GROUPNAME: CAPITALLETTER ( LETTER | DECIMALDIGIT | "_" )*

INTLIT    : DECIMALLIT | OCTALLIT | HEXLIT
DECIMALLIT: ( "1".."9" ) ( DECIMALDIGIT )*
OCTALLIT  : "0" ( OCTALDIGIT )*
HEXLIT    : "0" ( "x" | "X" ) HEXDIGIT ( HEXDIGIT )*

FLOATLIT: ( DECIMALS "." [ DECIMALS ] [ EXPONENT ] | DECIMALS EXPONENT | "."DECIMALS [ EXPONENT ] ) | "inf" | "nan"
DECIMALS : DECIMALDIGIT ( DECIMALDIGIT )*
EXPONENT : ( "e" | "E" ) [ "+" | "-" ] DECIMALS

BOOLLIT: "true" | "false"

STRLIT: ( "'" ( CHARVALUE )* "'" ) |  ( "\"" ( CHARVALUE )* "\"" )
CHARVALUE: HEXESCAPE | OCTESCAPE | CHARESCAPE |  /[^\0\n\\]/
HEXESCAPE: "\\" ( "x" | "X" ) HEXDIGIT HEXDIGIT
OCTESCAPE: "\\" OCTALDIGIT OCTALDIGIT OCTALDIGIT
CHARESCAPE: "\\" ( "a" | "b" | "f" | "n" | "r" | "t" | "v" | "\\" | "'" | "\"" )
QUOTE: "'" | "\""

EMPTYSTATEMENT: ";"

CONSTANT: FULLIDENT | ( [ "-" | "+" ] INTLIT ) | ( [ "-" | "+" ] FLOATLIT ) | STRLIT | BOOLLIT

syntax: "syntax" "=" QUOTE "proto2" QUOTE tail

import: "import" [ "weak" | "public" ] STRLIT tail

package: "package" FULLIDENT tail

option: [ comments ] "option" OPTIONNAME  "=" CONSTANT tail
OPTIONNAME: ( IDENT | "(" FULLIDENT ")" ) ( "." IDENT )*

LABEL: "required" | "optional" | "repeated"
TYPE: "double" | "float" | "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64" | "fixed32" | "fixed64" | "sfixed32" | "sfixed64" | "bool" | "string" | "bytes" | MESSAGETYPE | ENUMTYPE
FIELDNUMBER: INTLIT

field: [ comments ] LABEL TYPE FIELDNAME "=" FIELDNUMBER [ "[" fieldoptions "]" ] tail
fieldoptions: fieldoption ( ","  fieldoption )*
fieldoption: OPTIONNAME "=" CONSTANT

oneof: [ comments ] "oneof" ONEOFNAME "{" ( oneoffield | EMPTYSTATEMENT )* "}"
oneoffield: [ comments ] TYPE FIELDNAME "=" FIELDNUMBER [ "[" fieldoptions "]" ] tail
group: [ comments ] LABEL "group" GROUPNAME "=" FIELDNUMBER messagebody

mapfield: [ comments ] "map" "<" KEYTYPE "," TYPE ">" MAPNAME "=" FIELDNUMBER [ "[" fieldoptions "]" ] tail
KEYTYPE: "int32" | "int64" | "uint32" | "uint64" | "sint32" | "sint64" | "fixed32" | "fixed64" | "sfixed32" | "sfixed64" | "bool" | "string"

extensions: [ comments ] "extensions" ranges tail

reserved: [ comments ] "reserved" ( ranges | fieldnames ) tail
ranges: range ( "," range )*
range:  INTLIT [ "to" ( INTLIT | "max" ) ]
fieldnames: FIELDNAME ( "," FIELDNAME )*

enum: [ comments ] "enum" ENUMNAME enumbody
enumbody: "{" ( option | enumfield | reserved | EMPTYSTATEMENT )* "}"
enumfield: [ comments ] IDENT "=" [ "-" ] INTLIT [ "[" enumvalueoption ( ","  enumvalueoption )* "]" ] tail
enumvalueoption: OPTIONNAME "=" CONSTANT

message: [ comments ] "message" MESSAGENAME messagebody
messagebody: "{" ( field | enum | message | extend | extensions | option | oneof | mapfield | reserved | group | EMPTYSTATEMENT )* "}"
extend: [ comments ] "extend" MESSAGETYPE "{" (field | group)* "}"

service: [ comments ] "service" SERVICENAME "{" ( option | rpc | EMPTYSTATEMENT )* "}"
rpc: [ comments ] "rpc" RPCNAME "(" [ "stream" ] MESSAGETYPE ")" "returns" "(" [ "stream" ] MESSAGETYPE ")" ( ( "{" ( option | EMPTYSTATEMENT )* "}" ) | ";" )

proto:[ comments ] [ syntax ] ( import | package | option | topleveldef | EMPTYSTATEMENT )*
topleveldef: message | enum | extend | service

tail: ";" /[\s|\t]/* [ trail_comment ] NEWLINE
trail_comment: COMMENT
COMMENT: "//" /[^\n]/*
BLOCKCOMMENT: "/*" /./* "*/"
comments: (( COMMENT | BLOCKCOMMENT ) NEWLINE )+

%import common.HEXDIGIT
%import common.DIGIT -> DECIMALDIGIT
%import common.LETTER
%import common.WS
%import common.NEWLINE
%ignore WS
'''

Tail = typing.NamedTuple('Tail', [('comment', 'Comment')])
Comment = typing.NamedTuple('Comment', [('content', str), ('tags', typing.Dict[str, typing.Any]), ('ue_specifiers', str)])
Oneof = typing.NamedTuple('Oneof', [('comment', 'Comment'), ('name', str), ('fields', typing.List['Field'])])
FieldOption = typing.NamedTuple('FieldOption', [('name', str), ('content', str)])
Field = typing.NamedTuple('Field', [('comment', 'Comment'), ('label', str), ('type', str), ('key_type', str), ('val_type', str), ('name', str), ('number', int), ('options', typing.Dict[str, 'FieldOption']), ('user_data', typing.Dict[str, typing.Any])])
Enum = typing.NamedTuple('Enum', [('comment', 'Comment'), ('name', str), ('fields', typing.Dict[str, 'Field']), ('user_data', typing.Dict[str, typing.Any])])
Option = typing.NamedTuple('Option', [('comment', 'Comment'), ('name', str), ('content', str)])
Message = typing.NamedTuple('Message', [('comment', 'Comment'), ('name', str), ('fields', typing.List['Field']), ('oneofs', typing.List['Oneof']),
                                        ('messages', typing.Dict[str, 'Message']), ('enums', typing.Dict[str, 'Enum']), ('options', typing.Dict[str, 'Option']), ('user_data', typing.Dict[str, typing.Any])])
Service = typing.NamedTuple('Service', [('name', str), ('functions', typing.Dict[str, 'RpcFunc'])])
RpcFunc = typing.NamedTuple('RpcFunc', [('name', str), ('in_type', str), ('out_type', str), ('uri', str)])
ProtoFile = typing.NamedTuple('ProtoFile',
                              [('messages', typing.Dict[str, 'Message']), ('enums', typing.Dict[str, 'Enum']),
                               ('services', typing.Dict[str, 'Service']), ('imports', typing.List[str]),
                               ('options', typing.Dict[str, str]), ('package', str), ('user_data', typing.Dict[str, typing.Any])])


def merge_comments(comments):
    content = ""
    tags = {}
    ue_specifiers = None

    for comment in comments:
        content += comment.content
        for tag, value in comment.tags.items():
            tags[tag] = value
        if not ue_specifiers and comment.ue_specifiers:
            ue_specifiers = comment.ue_specifiers

    return Comment(content, tags, ue_specifiers)

def extrat_comments(tokens):
    comments = []
    for token in tokens:
        if isinstance(token, Comment):
            comments.append(token)
        elif isinstance(token, Tail):
            if token.comment:
                comments.append(token.comment)
        elif isinstance(token, Token):                
            if token.type == "COMMENT":
                comments.append(Comment(token.value, {}, None))  

    return merge_comments(comments)

class ProtoTransformer(Transformer):
    '''Converts syntax tree token into more easily usable namedtuple objects'''
    def message(self, tokens):
        '''Returns a Message namedtuple'''
        comment = Comment("", {}, None)     
        if len(tokens) < 3:
            name_token, body = tokens
        else:
            comment, name_token, body = tokens
        return Message(comment, name_token.value, *body, {})
    
    def oneof(self, tokens):
        '''Returns a Oneof namedtuple'''
        comment = Comment("", {})
        fields = []
        name = None
        for token in tokens:
            if isinstance(token, Comment):
                comment = token
            elif isinstance(token, Field):
                fields.append(token)
            elif isinstance(token, Token) and token.type == 'ONEOFNAME':
                name = token.value
        return Oneof(comment, name, fields)
    
    def oneoffield(self, tokens):
        '''Returns a Field namedtuple'''
        comment = Comment("", {})
        type = Token("TYPE", "")
        fieldname = Token("FIELDNAME", "")
        fieldnumber = Token("FIELDNUMBER", "")
        for token in tokens:
            if isinstance(token, Comment):
                comment = token
            elif isinstance(token, Token):
                if token.type == "TYPE":
                    type = token
                elif token.type == "FIELDNAME":
                    fieldname = token
                elif token.type == "FIELDNUMBER":
                    fieldnumber = token
                elif token.type == "COMMENT":
                    comment = Comment(token.value, {})
        return Field(comment, type.value, type.value, type.value, fieldname.value, int(fieldnumber.value))

    def fieldoption(self, tokens):
        name = Token("TYPE", "")
        content = Token("", "")
        for token in tokens:            
            if isinstance(token, Token):
                if token.type == "OPTIONNAME":
                    name.value = token.value.strip("()")
                if token.type == "CONSTANT":
                    content = token
        
        return FieldOption(name, content)

    def enumvalueoption(self, tokens):
        return self.fieldoption(tokens)

    def option(self, tokens):
        name = Token("TYPE", "")
        content = Token("", "")
        comment = extrat_comments(tokens)
        for token in tokens:
            if isinstance(token, Comment):
                comment = token
            elif isinstance(token, Token):
                if token.type == "OPTIONNAME":
                    name.value = token.value.strip("()")
                if token.type == "CONSTANT":
                    content = token
        
        return Option(comment, name, content)

    def messagebody(self, items):
        '''Returns a tuple of message body namedtuples'''
        messages = {}
        enums = {}
        fields = []
        options = {}
        oneofs = []
        for item in items:
            if isinstance(item, Message):
                messages[item.name] = item
            elif isinstance(item, Enum):
                enums[item.name] = item
            elif isinstance(item, Field):
                fields.append(item)
            elif isinstance(item, Option):
                options[item.name] = item
            elif isinstance(item, Oneof):
                oneofs.append(item)

        return fields, oneofs, messages, enums, options

    def tail(self, tokens):
        comment = None
        for token in tokens:
            if isinstance(token, Comment):
                comment = token

        return Tail(comment)

    def field(self, tokens):
        '''Returns a Field namedtuple'''        
        type = Token("TYPE", "")
        label = Token("LABEL", "")
        fieldname = Token("FIELDNAME", "")
        fieldnumber = Token("FIELDNUMBER", "")
        options = {}
        for token in tokens:            
            if isinstance(token, Tree) and token.data == 'fieldoptions':
                for fieldoption in token.children:
                    if isinstance(fieldoption, FieldOption):
                        options[fieldoption.name.value] = fieldoption
            elif isinstance(token, Token):
                if token.type == "TYPE":
                    type = token
                elif token.type == "LABEL":
                    label = token
                elif token.type == "FIELDNAME":
                    fieldname = token
                elif token.type == "FIELDNUMBER":
                    fieldnumber = token                

        return Field(extrat_comments(tokens), label.value, type.value, type.value, type.value, fieldname.value, int(fieldnumber.value), options, {})

    def mapfield(self, tokens):
        '''Returns a Field namedtuple'''        
        val_type = Token("TYPE", "")
        key_type = Token("KEYTYPE", "")
        fieldname = Token("MAPNAME", "")
        fieldnumber = Token("FIELDNUMBER", "")
        options = {}
        for token in tokens:           
            if isinstance(token, Tree) and token.data == 'fieldoptions':
                for fieldoption in token.children:
                    if isinstance(fieldoption, FieldOption):
                        options[token.name] = token 
            elif isinstance(token, Token):
                if token.type == "TYPE":
                    val_type = token
                elif token.type == "KEYTYPE":
                    key_type = token
                elif token.type == "MAPNAME":
                    fieldname = token
                elif token.type == "FIELDNUMBER":
                    fieldnumber = token                
        return Field(extrat_comments(tokens), '', 'map', key_type.value, val_type.value, fieldname.value, int(fieldnumber.value), options, {})

    def comments(self, tokens):
        '''Returns a Tag namedtuple'''
        comment = ''
        tags = {}
        ue_specifier = None
        for token in tokens:
            if token is None:
                continue

            token_str = ""
            if isinstance(token, Token):
                token_str = token.value
            else:
                token_str = token

            if token_str.find("//") >= 0:
                comment_content = token_str.replace("//", "").strip(" /\n")
                if comment_content.startswith("UPROPERTY") or comment_content.startswith("UCLASS") or comment_content.startswith("UENUM"):
                    ue_specifier = comment_content
                    continue

            comment += token_str + "\n"
            if token_str.find('@') < 0:
                continue
            kvs = token_str.strip(" /\n").split('@')
            for kv in kvs:
                kv = kv.strip(" /\n")
                if not kv:
                    continue
                tmp = kv.split('=')
                key = tmp[0].strip(" /\n").lower()
                if key.find(" ") >= 0:
                    continue
                if len(tmp) > 1:
                    tags[key] = tmp[1].lower()
                else:
                    tags[key] = True
        return Comment(comment, tags, ue_specifier)
    
    def trail_comment(self, tokens):        
        if len(tokens) > 0:
            return Comment(tokens[0].value, {}, None)
        else:
            return Comment("", {}, None)

    def enum(self, tokens):
        '''Returns an Enum namedtuple'''
        comment = Comment("", {}, None)
        if len(tokens) < 3:
            name, fields = tokens
        else:
            comment, name, fields = tokens
        return Enum(comment, name.value, fields, {})

    def enumbody(self, tokens):
        '''Returns a sequence of enum identifiers'''
        enumitems = []
        for tree in tokens:
            if tree.data != 'enumfield':
                continue
            name = Token("IDENT", "")
            value = Token("INTLIT", "")
            options = {}
            for token in tree.children:                
                if isinstance(token, Tree) and token.data == 'enumvalueoption':
                    for enumvalueoption in token.children:
                        if isinstance(enumvalueoption, FieldOption):
                            options[token.name] = token
                elif isinstance(token, Token):
                    if token.type == "IDENT":
                        name = token
                    elif token.type == "INTLIT":
                        value = token
            enumitems.append(Field(extrat_comments(tree.children), '', 'enum', 'enum', 'enum', name.value, value.value, options, {}))
        return enumitems

    def service(self, tokens):
        '''Returns a Service namedtuple'''
        functions = []
        name = ''
        for i in range(0, len(tokens)):
            if not isinstance(tokens[i], Comment):
                if isinstance(tokens[i], RpcFunc):
                    functions.append(tokens[i])
                else:
                    name = tokens[i].value
        return Service(name, functions)

    def rpc(self, tokens):
        '''Returns a RpcFunc namedtuple'''
        uri = ''
        in_type = ''
        for token in tokens:
            if isinstance(token, Token):
                if token.type == "RPCNAME":
                    name = token
                elif token.type == "MESSAGETYPE":
                    if in_type:
                        out_type = token
                    else:
                        in_type = token
            elif not isinstance(token, Comment):
                option_token = token
                uri = option_token.children[0].value
        return RpcFunc(name.value, in_type.value, out_type.value, uri.strip('"'))


def _recursive_to_dict(obj):
    _dict = {}

    if isinstance(obj, tuple):
        node = obj._asdict()
        for item in node:
            if isinstance(node[item], list):  # Process as a list
                _dict[item] = [_recursive_to_dict(x) for x in (node[item])]
            elif isinstance(node[item], tuple):  # Process as a NamedTuple
                _dict[item] = _recursive_to_dict(node[item])
            elif isinstance(node[item], dict):
                for k in node[item]:
                    if isinstance(node[item][k], tuple):
                        node[item][k] = _recursive_to_dict(node[item][k])
                _dict[item] = node[item]
            else:  # Process as a regular element
                _dict[item] = (node[item])
    return _dict


def parse_from_file(file: str, encoding: str="utf-8"):
    with open(file, 'r', encoding=encoding) as f:
        data = f.read()
    if data:
        return parse(data)


def parse(data: str):
    parser = Lark(BNF, start='proto', parser='earley', debug=True)    
    tree = parser.parse(data)
    trans_tree = ProtoTransformer().transform(tree)
    enums = {}
    messages = {}
    services = {}
    imports = []
    import_tree = trans_tree.find_data('import')
    for tree in import_tree:
        for child in tree.children:
            if isinstance(child, Token):
                imports.append(child.value.strip('"'))
    options = {}
    option_tree = trans_tree.find_data('option')
    for tree in option_tree:
        options[tree.children[0]] = tree.children[1].strip('"')

    package = ''
    package_tree = trans_tree.find_data('package')
    for tree in package_tree:
        package = tree.children[0]

    top_data = trans_tree.find_data('topleveldef')
    for top_level in top_data:
        for child in top_level.children:
            if isinstance(child, Message):
                messages[child.name] = child
            if isinstance(child, Enum):
                enums[child.name] = child
            if isinstance(child, Service):
                services[child.name] = child
    return ProtoFile(messages, enums, services, imports, options, package, {})


def serialize2json(data):
    return json.dumps(_recursive_to_dict(parse(data)))


def serialize2json_from_file(file: str, encoding: str="utf-8"):
    with open(file, 'r', encoding=encoding) as f:
        data = f.read()
    if data:
        return json.dumps(_recursive_to_dict(parse(data)), indent=4)