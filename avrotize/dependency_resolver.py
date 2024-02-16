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
    
    sorted_messages = []
    while avro_schema:
        found = False
        for record in avro_schema:
            if not any(record.get('name') in other_record.get('dependencies', []) 
                       or (record.get('namespace','')+'.'+record.get('name')) in other_record.get('dependencies', []) for other_record in avro_schema):
                if 'dependencies' in record:
                    del record['dependencies']
                sorted_messages.append(record)
                avro_schema.remove(record)
                found = True
        # If there are no records without dependencies, so we just take one record and move on
        if not found:
            record = avro_schema[0]
            swap_record_dependencies(avro_schema, record)

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

def swap_dependency_type(avro_schema, field, dependency, dependency_type, dependencies):
    """ to break circular dependencies, we will inline the dependent record and remove the dependency """
    if not dependency in dependencies:
        return
    if not dependency_type in avro_schema:
        return
    
    # Replace the dependency type with the dependency_type in avro_schema.
    if field['type'] == dependency:
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
            if field_type == dependency:
                field['type'].remove(dependency)
                field['type'].append(dependency_type)
                if dependency_type in avro_schema:
                    avro_schema.remove(dependency_type)
                dependencies.remove(dependency)
                dependencies.extend(dependency_type.get('dependencies', []))
                if 'dependencies' in dependency_type:
                    swap_record_dependencies(avro_schema, dependency_type)
                    del dependency_type['dependencies']
            # type is an object?
            elif type(field_type) is dict and field_type.get('type') != None and field_type.get('name') == dependency:
                swap_dependency_type(avro_schema, field_type, dependency, dependency_type, dependencies)
    elif 'type' in field['type']:
        swap_dependency_type(avro_schema, field['type'], dependency, dependency_type, dependencies)
    elif field['type'] == 'array':
            if field['items'] == dependency:
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
        if field['values'] == dependency:
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
