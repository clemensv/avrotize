import json
import os
import subprocess
import argparse
from typing import List, Dict

INDENT = '    '

def get_latest_git_tag() -> str:
    """
    Gets the latest Git tag from the repository.

    Returns:
        str: The latest Git tag.
    """
    try:
        latest_tag = subprocess.check_output(['git', 'describe', '--tags', '--abbrev=0']).strip().decode('utf-8')
        return latest_tag
    except subprocess.CalledProcessError:
        return "0.0.1"

def update_vs_code_extension_project(root_path: str, json_file_path: str) -> None:
    """
    Updates the VS Code extension project based on the commands in the given JSON file and the latest Git tag.
    
    Args:
        root_path (str): The root path of the VS Code extension project.
        json_file_path (str): The path to the JSON file with command definitions.
    """
    # Load the command definitions from the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        commands = json.load(file)

    # Update the package.json context menu section and version
    package_json_path = os.path.join(root_path, 'package.json')
    with open(package_json_path, 'r', encoding='utf-8') as file:
        package_json = json.load(file)

    # Get the latest Git tag for version
    latest_version = get_latest_git_tag()
    package_json['version'] = latest_version

    # Clear existing context menu entries
    package_json['contributes']['menus']['editor/context'] = []

    # Add new context menu entries under a common "Convert >" parent
    convert_submenus = []
    all_extensions = set()
    for command in commands:
        command_description = command['description']
        # strip all text before the first occurence of ' to ' in the command description
        command_description = clip_command_description(command_description)
        all_extensions.update(command['extensions'])
        ext_conditions = " || ".join([f"resourceExtname == {ext}" for ext in command['extensions']])
        convert_submenus.append({
            "command": f"avrotize.{command['command']}",
            "group": "navigation",
            "when": ext_conditions,
            "title": clip_command_description(command['description'])
        })
    
    package_json['contributes']['menus']['editor/context'].append({
        "submenu": "convertSubmenu",
        "title": "Convert",
        "group": "navigation",
        "when": " || ".join([f"resourceExtname == {ext}" for ext in all_extensions])
    })
    package_json['contributes']['menus']['convertSubmenu'] = convert_submenus
    package_json['contributes']['submenus'] = [{
                "id": "convertSubmenu",
                "label": "Convert"
            }]
    package_json['contributes']['commands'] = [
        {
            "command": f"avrotize.{command['command']}",
            "title": clip_command_description(command['description']),
            "category": "Convert"
        }
        for command in commands
    ]

    # Save the updated package.json
    with open(package_json_path, 'w', encoding='utf-8') as file:
        json.dump(package_json, file, indent=4)

    # Create the src/extension.ts content
    extension_ts_path = os.path.join(root_path, 'src', 'extension.ts')
    extension_ts_content = [
        "import * as vscode from 'vscode';",
        "import { exec } from 'child_process';",
        "import * as path from 'path';",
        "",
     ]

    extension_ts_content.append(f"async function checkAvrotizeTool(context: vscode.ExtensionContext) {{")
    extension_ts_content.append(f"{INDENT}try {{")
    extension_ts_content.append(f"{INDENT*2}exec('avrotize -h', async (error, stdout, stderr) => {{")
    extension_ts_content.append(f"{INDENT*3}if (error) {{")
    extension_ts_content.append(f"{INDENT*4}const installOption = await vscode.window.showWarningMessage(")
    extension_ts_content.append(f"{INDENT*5}'avrotize tool is not available. Do you want to install it?', 'Yes', 'No');")
    extension_ts_content.append(f"{INDENT*4}if (installOption === 'Yes') {{")
    extension_ts_content.append(f"{INDENT*5}if (!await isPythonAvailable()) {{")
    extension_ts_content.append(f"{INDENT*6}vscode.window.showErrorMessage('Python 3.11 or higher must be installed.');")
    extension_ts_content.append(f"{INDENT*6}return;")
    extension_ts_content.append(f"{INDENT*5}}}")
    extension_ts_content.append(f"{INDENT*5}await createVenvAndInstall(context.extensionPath);")
    extension_ts_content.append(f"{INDENT*5}vscode.window.showInformationMessage('avrotize tool has been installed successfully.');")
    extension_ts_content.append(f"{INDENT*4}}}")
    extension_ts_content.append(f"{INDENT*3}}}")
    extension_ts_content.append(f"{INDENT*2}}});")
    extension_ts_content.append(f"{INDENT}}} catch (error) {{")
    extension_ts_content.append(f"{INDENT*2}vscode.window.showErrorMessage('Error checking avrotize tool availability: ' + error);")
    extension_ts_content.append(f"{INDENT}}}")
    extension_ts_content.append("}")

    extension_ts_content.append(f"async function isPythonAvailable(): Promise<boolean> {{")
    extension_ts_content.append(f"{INDENT}try {{")
    extension_ts_content.append(f"{INDENT*2}const output = await execShellCommand('python3 --version');")
    extension_ts_content.append(f"{INDENT*2}const version = output.trim().split(' ')[1];")
    extension_ts_content.append(f"{INDENT*2}const [major, minor] = version.split('.').map(num => parseInt(num));")
    extension_ts_content.append(f"{INDENT*2}return major === 3 && minor >= 11;")
    extension_ts_content.append(f"{INDENT}}} catch {{")
    extension_ts_content.append(f"{INDENT*2}return false;")
    extension_ts_content.append(f"{INDENT}}}")
    extension_ts_content.append("}")

    extension_ts_content.append(f"async function createVenvAndInstall(extensionPath: string) {{")
    extension_ts_content.append(f"{INDENT}const venvPath = path.join(extensionPath, 'venv');")
    extension_ts_content.append(f"{INDENT}await execShellCommand(`python3 -m venv ${{venvPath}}`);")
    extension_ts_content.append(f"{INDENT}const pipPath = path.join(venvPath, 'bin', 'pip');")
    extension_ts_content.append(f"{INDENT}await execShellCommand(`${{pipPath}} install avrotize`);")
    extension_ts_content.append("}")

    extension_ts_content.append(f"function execShellCommand(cmd: string): Promise<string> {{")
    extension_ts_content.append(f"{INDENT}return new Promise((resolve, reject) => {{")
    extension_ts_content.append(f"{INDENT*2}exec(cmd, (error, stdout, stderr) => {{")
    extension_ts_content.append(f"{INDENT*3}if (error) {{")
    extension_ts_content.append(f"{INDENT*4}reject(stderr);")
    extension_ts_content.append(f"{INDENT*3}}} else {{")
    extension_ts_content.append(f"{INDENT*4}resolve(stdout);")
    extension_ts_content.append(f"{INDENT*3}}}")
    extension_ts_content.append(f"{INDENT*2}}});")
    extension_ts_content.append(f"{INDENT}}});")
    extension_ts_content.append("}")

    extension_ts_content.append(f"function executeCommand(command: string) {{")
    extension_ts_content.append(f"{INDENT}exec(command, (error, stdout, stderr) => {{")
    extension_ts_content.append(f"{INDENT*2}if (error) {{")
    extension_ts_content.append(f"{INDENT*3}vscode.window.showErrorMessage(`Error: ${{stderr}}`);")
    extension_ts_content.append(f"{INDENT*2}}} else {{")
    extension_ts_content.append(f"{INDENT*3}vscode.window.showInformationMessage(`Success: ${{stdout}}`);")
    extension_ts_content.append(f"{INDENT*2}}}")
    extension_ts_content.append(f"{INDENT}}});")
    extension_ts_content.append("}")
    
    extension_ts_content.extend(
        [
            "export function activate(context: vscode.ExtensionContext) {",
            f"{INDENT}const disposables: vscode.Disposable[] = [];",
            "",
            f"{INDENT}checkAvrotizeTool(context);",
            ""
        ]
    )

    # Add command implementations
    for command in commands:
        file_basename_defined = False
        extension_ts_content.append(f"{INDENT}disposables.push(vscode.commands.registerCommand('avrotize.{command['command']}', async (uri: vscode.Uri) => {{")
        extension_ts_content.append(f"{INDENT*2}const filePath = uri.fsPath;")
        
        args_str = ''
        output_prompt = ''
        prompts = command.get('prompts', [])
        
        for arg in command['args']:
            if arg.get('name') == "input":
                args_str += "${filePath}"
            elif arg.get('name') == "--out":
                filter_extensions = f"'All Files': ['*']"
                suggested_output_path = command.get('suggested_output_file_path', '')
                if suggested_output_path:
                    splitext = suggested_output_path.split('.',1)
                    if len(splitext) > 1:
                        ext = splitext[1]
                        if ext: 
                            filter_extensions = f"'{ext} File': ['{ext}']"
                    extension_ts_content.append(f"{INDENT*2}const outputPathSuggestion = getSuggestedOutputPath(filePath, '{suggested_output_path}');")
                output_prompt = f"{INDENT*2}const outputPath = await vscode.window.showSaveDialog(" + \
                    f"{{ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : {{ {filter_extensions} }} }});"
                args_str += f" --out ${{outputPath.fsPath}}"
        
        for prompt in prompts:
            prompt_var_name = prompt['name'][2:].replace('-', '_').lower()+ '_value'
            prompt_message = prompt.get('message', f"Enter the value for {prompt['name']}").replace("'", "\\'")
            choices = prompt.get('choices', [])
            arg = next((a for a in command['args'] if a['name'] == prompt['name']), None)
            
            if choices:
                choices_str = ', '.join([f"'{c}'" for c in choices])
                extension_ts_content.append(f"{INDENT*2}const {prompt_var_name} = await vscode.window.showQuickPick([{choices_str}], {{ placeHolder: '{prompt_message}' }});")
            else:
                if arg.get('type') == 'bool':
                    extension_ts_content.append(f"{INDENT*2}const {prompt_var_name} = await vscode.window.showQuickPick(['Yes', 'No'], {{ title: '{prompt_message}' }}) === 'Yes';")
                else:
                    default_value = prompt.get('default', '')
                    if isinstance(default_value, bool):
                        extension_ts_content.append(f"{INDENT*2}const {prompt_var_name}_default_value = {'Yes' if default_value else 'No'};")
                    elif default_value:
                        if not file_basename_defined:
                            extension_ts_content.append(f"{INDENT*2}const fileBaseName = path.basename(filePath, path.extname(filePath));")
                            file_basename_defined = True
                        extension_ts_content.append(f"{INDENT*2}const {prompt_var_name}_default_value = '{default_value}'.replace('{{input_file_name}}', fileBaseName);")
                    line = f"{INDENT*2}const {prompt_var_name} = await vscode.window.showInputBox({{ prompt: '{prompt_message}'" 
                    if default_value:
                        line += f", value: `${{ {prompt_var_name}_default_value }}` }});"
                    else:
                        line += " });"
                    extension_ts_content.append(line)
            
            if arg.get('type') == 'bool':
                extension_ts_content.append(f"{INDENT*2}const {prompt_var_name}_arg = {prompt_var_name} ? '{prompt['name']}' : '';")
                args_str += f" ${{{prompt_var_name}_arg}}"
            else:
                args_str += f" {prompt['name']} ${{{prompt_var_name}}}"
                
        if output_prompt:
            extension_ts_content.append(output_prompt)
            extension_ts_content.append(f"{INDENT*2}if (!outputPath) {{ return; }}")
        extension_ts_content.append(f"{INDENT*2}const command = `avrotize {command['command']} {args_str}`;")
        extension_ts_content.append(f"{INDENT*2}executeCommand(command);")
        extension_ts_content.append(f"{INDENT}}}));")
        extension_ts_content.append("")

    # Finalize the src/extension.ts content
    extension_ts_content.append(f"{INDENT}context.subscriptions.push(...disposables);")
    extension_ts_content.append("}")
    extension_ts_content.append("")
    extension_ts_content.append("export function deactivate() {}")
    extension_ts_content.append("\nfunction getSuggestedOutputPath(inputFilePath: string, suggestedOutputPath: string) {")
    extension_ts_content.append(f"{INDENT}const inputFileName = inputFilePath ? path.basename(inputFilePath, path.extname(inputFilePath)) : '';")
    extension_ts_content.append(f"{INDENT}const outFileName = suggestedOutputPath.replace('{{input_file_name}}', inputFileName);")
    extension_ts_content.append(f"{INDENT}return path.join(path.dirname(inputFilePath), outFileName);")
    extension_ts_content.append("}")

    # Write the src/extension.ts content to file
    with open(extension_ts_path, 'w', encoding='utf-8') as file:
        file.write("\n".join(extension_ts_content))

def clip_command_description(command_description):
    if ' to ' in command_description:
        command_description = command_description[command_description.find(' to ')+1:]
    return command_description

def main():
    """
    Main function to parse arguments and update the VS Code extension project.
    """
    parser = argparse.ArgumentParser(description='Update VS Code extension project based on JSON file commands.')
    parser.add_argument('--extension-root', type=str, help='The root path of the VS Code extension project.')
    parser.add_argument('--commands', type=str, help='The path to the JSON file with command definitions.')
    
    args = parser.parse_args()
    update_vs_code_extension_project(args.extension_root, args.commands)

if __name__ == "__main__":
    main()
