"""

Command line utility to convert a variety of schema formats to Avrotize schema and vice versa.

"""


import argparse
import tempfile
import sys
import os
import json
from avrotize import _version

def load_commands():
    """Load the commands from the commands.json file."""
    commands_path = os.path.join(os.path.dirname(__file__), 'commands.json')
    with open(commands_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_subparsers(subparsers, commands):
    """Create subparsers for the commands."""
    for command in commands:
        cmd_parser = subparsers.add_parser(command['command'], help=command['description'])
        for arg in command['args']:
            kwargs = {
                'type': eval(arg['type']),
                'help': arg['help'],
            }
            
            if 'nargs' in arg:
                kwargs['nargs'] = arg['nargs']
            if 'choices' in arg:
                kwargs['choices'] = arg['choices']
            if 'default' in arg:
                kwargs['default'] = arg['default']
            if arg['type'] == 'bool':
                kwargs['action'] = 'store_true'
                del kwargs['type']
                carg = cmd_parser.add_argument(arg['name'], **kwargs)
            else:
                carg = cmd_parser.add_argument(arg['name'], **kwargs)
            carg.required = arg.get('required', True)

def dynamic_import(module, func):
    """Dynamically import a module and function."""
    mod = __import__(module, fromlist=[func])
    return getattr(mod, func)

def main():
    """Main function for the command line utility."""
    commands = load_commands()
    parser = argparse.ArgumentParser(description='Convert a variety of schema formats to Avrotize schema and vice versa.')
    parser.add_argument('--version', action='store_true', help='Print the version of Avrotize.')

    subparsers = parser.add_subparsers(dest='command')
    create_subparsers(subparsers, commands)

    args = parser.parse_args()

    if 'version' in args and args.version:
        print(f'Avrotize {_version.version}')
        return
    
    if args.command is None:
        parser.print_help()
        return
    
    try:
        command = next((cmd for cmd in commands if cmd['command'] == args.command), None)
        if not command:
            print(f"Error: Command {args.command} not found.")
            exit(1)
        
        input_file_path = args.input or getattr(args, 'avsc', None) or getattr(args, 'proto', None) or getattr(args, 'jsons', None) or getattr(args, 'xsd', None) or getattr(args, 'kusto_uri', None) or getattr(args, 'parquet', None) or getattr(args, 'asn', None) or getattr(args, 'kstruct', None)
        temp_input = None
        skip_input_file_handling = command.get('skip_input_file_handling', False)
        if not skip_input_file_handling:
            if input_file_path is None:
                temp_input = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                input_file_path = temp_input.name
                # read to EOF
                s = sys.stdin.read()
                while s:
                    temp_input.write(s)
                    s = sys.stdin.read()
                temp_input.flush()
                temp_input.close()

        suppress_print = False
        temp_output = None
        output_file_path = ''
        if 'out' in args:
            output_file_path = args.out
            if output_file_path is None:
                suppress_print = True
                temp_output = tempfile.NamedTemporaryFile(delete=False)
                output_file_path = temp_output.name
        
        def printmsg(s):
            if not suppress_print:
                print(s)
    
        module_name, func_name = command['function']['name'].rsplit('.', 1)
        func = dynamic_import(module_name, func_name)
        func_args = {}
        for arg in command['function']['args']:
            if command['function']['args'][arg] == 'input_file_path':
                func_args[arg] = input_file_path
            elif output_file_path and command['function']['args'][arg] == 'output_file_path':
                func_args[arg] = output_file_path
            else:
                val = command['function']['args'][arg]
                if val.startswith('args.'):
                    if hasattr(args, val[5:]):
                        func_args[arg] = getattr(args, val[5:])
                else:
                    func_args[arg] = val
        if output_file_path:
            printmsg(f'Executing {command["description"]} with input {input_file_path} and output {output_file_path}')
        func(**func_args)
        
        if temp_output:
            with open(output_file_path, 'r', encoding='utf-8') as f:
                sys.stdout.write(f.read())
            temp_output.close()

    except Exception as e:
        print("Error: ", str(e))
        exit(1)
    finally:
        if temp_input:
            try:
                os.remove(temp_input.name)
            except OSError as e:
                print(f"Error: Could not delete temporary input file {temp_input.name}. {e}")

if __name__ == "__main__":
    main()
