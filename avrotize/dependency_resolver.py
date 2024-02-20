# sort the dependencies

def inline_dependencies_of(avro_schema, record):
    for dependency in record.get('dependencies', []):
        dependency_type = next((x for x in avro_schema if x['name'] == dependency or x.get('namespace','')+'.'+x['name'] == dependency), None)
        if not dependency_type:
            continue
        deps = record.get('dependencies', [])
        for field in record['fields']:                        
            if record['name'] != dependency:
                swap_dependency_type(avro_schema, field, dependency, dependency_type, deps)
        record['dependencies'] = [dep for dep in deps if dep != record['name'] and record.get('namespace','')+'.'+record['name'] != dep]
    if 'dependencies' in record:
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
    while avro_schema:
        found = False
        for record in avro_schema:
            if not isinstance(record, dict):
                sorted_messages.append(record)
                avro_schema.remove(record)
                continue
            
            # if this record is not a dependency of any other record, it can be safely emitted now
            if not any(record.get('name') in other_record.get('dependencies', []) 
                       or (record.get('namespace','')+'.'+record.get('name')) in other_record.get('dependencies', []) for other_record in [x for x in avro_schema if isinstance(x, dict) and 'name' in x]):
                if 'dependencies' in record:
                    del record['dependencies']
                sorted_messages.append(record)
                avro_schema.remove(record)
                found = True
                
        # If there are no records without dependencies, we will grab the first 
        # record with dependencies and start resolving circular dependencies
        if len(avro_schema) > 0 and not found:
            record = next((x for x in avro_schema if isinstance(x, dict) and 'dependencies' in x), None)
            if record:
                avro_schema_len = len(avro_schema)
                swap_record_dependencies(avro_schema, record)
                if len(avro_schema) == avro_schema_len:
                    inline_dependencies_of(avro_schema, record)
        
    sorted_messages.reverse()
    return sorted_messages

def swap_record_dependencies(avro_schema, record):
    for dependency in record.get('dependencies', []):
        dependency_type = next((x for x in avro_schema if x['name'] == dependency or x.get('namespace','')+'.'+x['name'] == dependency), None)
        if not dependency_type:
            continue
        deps = record.get('dependencies', [])
        for field in record['fields']:                        
            if record['name'] != dependency and (record.get('namespace','')+'.'+record['name']) != dependency:
                swap_dependency_type(avro_schema, field, dependency, dependency_type, deps)
        record['dependencies'] = [dep for dep in deps if dep != record['name'] and record.get('namespace','')+'.'+record['name'] != dep]

def strip_namespace(name):
    if isinstance(name, str):
        return name.split('.')[-1]
    return name

def swap_dependency_type(avro_schema, field, dependency, dependency_type, dependencies):
    """ to break circular dependencies, we will inline the dependent record and remove the dependency """
    if not dependency in dependencies:
        return
    if not dependency_type in avro_schema:
        return
    
    # Replace the dependency type with the dependency_type in avro_schema.
    if strip_namespace(field['type']) == dependency:
        field['type'] = dependency_type
        if dependency_type in avro_schema:
            avro_schema.remove(dependency_type)
        dependencies.remove(dependency)
        dependencies.extend(dependency_type.get('dependencies', []))
        if 'dependencies' in dependency_type:
            swap_record_dependencies(avro_schema, dependency_type)
            del dependency_type['dependencies']
    # type is a Union?
    elif type(field['type']) is list:
        for field_type in field['type']:
            if strip_namespace(field_type) == dependency:
                field['type'].remove(field_type)
                field['type'].append(dependency_type)
                if dependency_type in avro_schema:
                    avro_schema.remove(dependency_type)
                dependencies.remove(dependency)
                dependencies.extend(dependency_type.get('dependencies', []))
                if 'dependencies' in dependency_type:
                    swap_record_dependencies(avro_schema, dependency_type)
                    del dependency_type['dependencies']
            # type is an object?
            elif isinstance(field_type, dict) and 'type' in field_type and field_type.get('type') == dependency or \
                'items' in field_type and field_type.get('items') == dependency or \
                'values' in field_type and field_type.get('values') == dependency:
                swap_dependency_type(avro_schema, field_type, dependency, dependency_type, dependencies)
    elif 'type' in field['type']:
        swap_dependency_type(avro_schema, field['type'], dependency, dependency_type, dependencies)
    elif field['type'] == 'array':
            if strip_namespace(field['items']) == dependency:
                field['items'] = dependency_type
                if dependency_type in avro_schema:
                    avro_schema.remove(dependency_type)
                dependencies.remove(dependency)
                dependencies.extend(dependency_type.get('dependencies', []))
                if 'dependencies' in dependency_type:
                    swap_record_dependencies(avro_schema, dependency_type)
                    del dependency_type['dependencies']
            elif 'type' in field['items']:
                swap_dependency_type(avro_schema, field['items'], dependency, dependency_type, dependencies)
    elif field['type'] == 'map':
        if strip_namespace(field['values']) == dependency:
            field['values'] = dependency_type
            if dependency_type in avro_schema:
                avro_schema.remove(dependency_type)                    
            dependencies.remove(dependency)
            dependencies.extend(dependency_type.get('dependencies', []))
            if 'dependencies' in dependency_type:
                swap_record_dependencies(avro_schema, dependency_type)
                del dependency_type['dependencies']
        elif 'type' in field['values']:
            swap_dependency_type(avro_schema, field['values'], dependency, dependency_type, dependencies)
    elif field['type'] == 'record':
        for dep_field in field['fields']:
            swap_dependency_type(avro_schema, dep_field, dependency, dependency_type, dependencies)
