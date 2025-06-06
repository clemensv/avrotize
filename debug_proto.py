from avrotize.chained_converters import convert_structure_to_proto
import tempfile
import os

struct_file = 'test/structs/sample.struct.json'
output_file = os.path.join(tempfile.gettempdir(), 'test_proto.proto')
print(f'Converting {struct_file} to {output_file}')

try:
    convert_structure_to_proto(struct_file, output_file)
    print(f'Output file size: {os.path.getsize(output_file) if os.path.exists(output_file) else "File not found"}')
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            content = f.read()
            print(f'Content: {content}')
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
