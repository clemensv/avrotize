# sort the dependencies

import copy
from typing import List


def inline_dependencies_of(avro_schema, record):
    for dependency in record.get('dependencies', []):
        dependency_type = next((x for x in avro_schema if x['name'] == dependency or x.get('namespace','')+'.'+x['name'] == dependency), None)
        if not dependency_type:
            continue
        deps = record.get('dependencies', [])
        for field in record['fields']:                        
            swap_dependency_type(avro_schema, field, dependency, dependency_type, deps, [record['namespace']+'.'+record['name']])
    del record['dependencies']

    

def sort_messages_by_dependencies(avro_schema):
    """ 
        Sort the messages in avro_schema by their dependencies. Avro Schema requires
        that type definitions must be defined before they are used. This method
        ensures this. Types that have dependencies will be moved at the end of the list.
        If necessary, it will also resolve circular dependencies by inlining the 
        dependent record.

        The method expects all types with dependencies to have a 'dependencies' key in their
        dict that contains a list of types that they depend on.

        Args:
            avro_schema: List of Avro schema records.
    """

    # if all are just strings, then it is already sorted
    if all(isinstance(record, str) for record in avro_schema):
        return avro_schema

    sorted_messages = []
    record_stack = []
    while avro_schema:
        found = False
        for record in avro_schema:
            if not isinstance(record, dict):
                sorted_messages.append(record)
                avro_schema.remove(record)
                continue
            
            # if this record is not a dependency of any other record, it can be safely emitted now
            #if not any(record.get('namespace','')+'.'+record.get('name') in other_record.get('dependencies', []) for other_record in [x for x in avro_schema if isinstance(x, dict) and 'name' in x]):
            remaining_deps = [dep for dep in record['dependencies'] if not dep in [x.get('namespace','')+'.'+x.get('name','') for x in sorted_messages]] if 'dependencies' in record else []
            if len(remaining_deps) == 0:
                if 'dependencies' in record:
                    del record['dependencies']
                sorted_messages.append(record)
                avro_schema.remove(record)
                found = True
                
        # If there are no records without dependencies, we will grab the first 
        # record with dependencies and start resolving circular dependencies
        if len(avro_schema) > 0 and not found:
            found = False
            for record in avro_schema:
                if isinstance(record, dict) and 'dependencies' in record:
                    remaining_deps = [dep for dep in record['dependencies'] if not dep in [x.get('namespace','')+'.'+x.get('name','') for x in sorted_messages]]
                    if len(remaining_deps) > 0:
                        swap_record_dependencies(avro_schema, record, [record.get('namespace','')+'.'+record['name']], 0)
                        if 'dependencies' in record and len(record['dependencies']) == 0:
                            del record['dependencies']
                        if isinstance(record, dict) and not 'dependencies' in record:
                            found = True
                            sorted_messages.append(record)
                            if record in avro_schema:
                                avro_schema.remove(record)
                            break
                        else:
                            remaining_remaining_deps = [dep for dep in record['dependencies'] if not dep in [x.get('namespace')+'.'+x.get('name') for x in sorted_messages]]
                            found = len(remaining_deps) != len(remaining_remaining_deps)
                            if found:
                                break
                            
            if not found:
                found = False
                for record in avro_schema:
                    if isinstance(record, dict) and 'dependencies' in record:            
                        found = True
                        record_deps = copy.deepcopy(record.get('dependencies', []))
                        inline_dependencies_of(avro_schema, record)
                        # fix the dependencies of all records that have this record as a dependency
                        for remaining_schema in avro_schema:
                            if isinstance(remaining_schema, dict) and 'dependencies' in remaining_schema and any(dep in record_deps for dep in remaining_schema['dependencies']):
                                remaining_schema['dependencies'] = [dep for dep in remaining_schema['dependencies'] if not dep in record_deps]
                                qname = record['namespace']+'.'+record['name']
                                if not qname in remaining_schema['dependencies']:
                                    remaining_schema['dependencies'].append(qname)
                        break

                if not found:
                    print('WARNING: There are circular dependencies in the schema, unable to resolve them: {}'.format([x['name'] for x in avro_schema if isinstance(x, dict) and 'dependencies' in x]))
    return sorted_messages

def swap_record_dependencies(avro_schema, record, record_stack: List[str], recursion_depth: int = 0):
    record_stack.append(record.get('namespace', '')+'.'+record['name'])
    if 'dependencies' in record:
        prior_dependencies = copy.deepcopy(record['dependencies'])
        while 'dependencies' in record and len(record['dependencies']) > 0:
            if 'fields' in record:
                for field in record['fields']:
                    if isinstance(field['type'], list):
                        for item in field['type'].copy():
                            sub_field = {
                                'type': item,
                                'name': field['name']   
                            }
                            resolve_field_dependencies(avro_schema, record, sub_field, record_stack, recursion_depth + 1)
                            if sub_field['type'] != item:
                                idx = field['type'].index(item)
                                field['type'].remove(item)
                                field['type'].insert(idx, sub_field['type'])
                    else:
                        resolve_field_dependencies(avro_schema, record, field, record_stack, recursion_depth + 1)
            if 'dependencies' in record and len(record['dependencies']) > 0:
                # compare the prior dependencies to the current dependencies one-by-one. If they are the same,
                # then we have a circular dependency.
                if prior_dependencies == record['dependencies']:
                    print('WARNING: Unable to resolve circular dependency in {}::{} with dependencies: {}'.format(record.get('namespace',''), record['name'], record['dependencies']))
                    break
                prior_dependencies = record['dependencies']
        if 'dependencies' in record:
            del record['dependencies']
    record_stack.pop()

def resolve_field_dependencies(avro_schema, record, field, record_stack, recursion_depth: int = 0):
    for dependency in record.get('dependencies', []):
        dependency_type = next((x for x in avro_schema if x['name'] == dependency or x.get('namespace','')+'.'+x['name'] == dependency), None)
        if not dependency_type and dependency in record['dependencies']:
            record['dependencies'].remove(dependency)
            continue
        deps = record.get('dependencies', [])
        if dependency_type:
            if record['name'] != dependency and (record.get('namespace','')+'.'+record['name']) != dependency:
                swap_dependency_type(avro_schema, field, dependency, dependency_type, deps, record_stack, recursion_depth + 1)
        record['dependencies'] = [dep for dep in deps if dep != record['name'] and record.get('namespace','')+'.'+record['name'] != dep]
        if len(record['dependencies']) == 0:
            del record['dependencies']


def swap_dependency_type(avro_schema, field, dependency, dependency_type, dependencies, record_stack: List[str], recursion_depth: int = 0):
    """ to break circular dependencies, we will inline the dependent record and remove the dependency """
    if not dependency in dependencies:
        return
    if not dependency_type in avro_schema:
        return
    if record_stack and dependency in record_stack:
        dependencies.remove(dependency)
        return
    
    # Replace the dependency type with the dependency_type in avro_schema.
    if isinstance(field['type'],str) and field['type'] == dependency:
        if dependency_type in avro_schema:
            field['type'] = dependency_type
            avro_schema.remove(dependency_type)
        dependencies.remove(dependency)
        dependencies.extend(dependency_type.get('dependencies', []))
        if 'dependencies' in dependency_type:
            swap_record_dependencies(avro_schema, dependency_type, record_stack, recursion_depth + 1)
           
    # type is a Union?
    elif isinstance(field['type'], list):
        for field_type in field['type']:
            if field_type == dependency:
                if dependency_type in avro_schema:
                    field['type'].remove(field_type)
                    field['type'].append(dependency_type)
                    avro_schema.remove(dependency_type)
                dependencies.remove(dependency)
                dependencies.extend(dependency_type.get('dependencies', []))
        for field_type in field['type']:
            if isinstance(field_type, dict):
                swap_dependency_type(avro_schema, field_type, dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)
    elif isinstance(field['type'], dict) and 'type' in field['type']:
        swap_dependency_type(avro_schema, field['type'], dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)
    elif field['type'] == 'array':
        if not 'items' in field:
            return
        if isinstance(field['items'], list):
            for item in field['items']:
                if item == dependency:
                    if dependency_type in avro_schema:
                        field['items'].remove(item)
                        field['items'].append(dependency_type)
                        avro_schema.remove(dependency_type)
                    dependencies.remove(dependency)
                    dependencies.extend(dependency_type.get('dependencies', []))
            for item in field['items']:
                if isinstance(item, dict):
                    swap_dependency_type(avro_schema, item, dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)               
        elif field['items'] == dependency:
            if dependency_type in avro_schema:
                field['items'] = dependency_type
                avro_schema.remove(dependency_type)
            dependencies.remove(dependency)
            dependencies.extend(dependency_type.get('dependencies', []))
            if 'dependencies' in dependency_type:
                swap_record_dependencies(avro_schema, dependency_type, record_stack)
        elif isinstance(field['items'], dict) and 'type' in field['items']:
            swap_dependency_type(avro_schema, field['items'], dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)
    elif field['type'] == 'map':
        if isinstance(field['values'], list):
            for item in field['values']:
                if item == dependency:
                    if dependency_type in avro_schema:
                        field['values'].remove(item)
                        field['values'].append(dependency_type)
                        avro_schema.remove(dependency_type)
                    dependencies.remove(dependency)
                    dependencies.extend(dependency_type.get('dependencies', []))
            for item in field['values']:
                if isinstance(item, dict):
                    swap_dependency_type(avro_schema, item, dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)
        if field['values'] == dependency:
            if dependency_type in avro_schema:
                field['values'] = dependency_type
                avro_schema.remove(dependency_type)                    
            dependencies.remove(dependency)
            dependencies.extend(dependency_type.get('dependencies', []))
            if 'dependencies' in dependency_type:
                swap_record_dependencies(avro_schema, dependency_type, record_stack)
        elif 'type' in field['values']:
            swap_dependency_type(avro_schema, field['values'], dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)
    elif field['type'] == 'record':
        record_stack.append(field.get('namespace', '')+'.'+field['name'])
        for dep_field in field['fields']:
            if isinstance(dep_field, dict):
                swap_dependency_type(avro_schema, dep_field, dependency, dependency_type, dependencies, record_stack, recursion_depth + 1)  
        record_stack.pop()
