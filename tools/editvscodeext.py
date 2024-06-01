import json
import os
import subprocess
import argparse
from typing import List, Dict

INDENT = '   '

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
    for command in commands:
        ext_conditions = " || ".join([f"resourceExtname == '{ext}'" for ext in command['extensions']])
        convert_submenus.append({
            "command": f"avrotize.{command['command']}",
            "group": "navigation",
            "when": ext_conditions,
            "title": command['description']
        })
    
    package_json['contributes']['menus']['editor/context'].append({
        "submenu": "convertSubmenu",
        "title": "Convert >",
        "group": "navigation"
    })
    package_json['contributes']['menus']['convertSubmenu'] = convert_submenus

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
        "export function activate(context: vscode.ExtensionContext) {",
        f"{INDENT}const disposables: vscode.Disposable[] = [];",
        ""
    ]


    # Add command implementations
    for command in commands:
        extension_ts_content.append(f"{INDENT}disposables.push(vscode.commands.registerCommand('avrotize.{command['command']}', async (uri: vscode.Uri) => {{")
        extension_ts_content.append(f"{INDENT*2}const filePath = uri.fsPath;")
        args_str = ''
        output_prompt = ''
        prompts = command.get('prompts', [])
        for arg in [a for a in command['args'] if a not in prompts]:
            if arg.get('name') == "input":
                args_str += "${filePath}"
            elif arg.get('name') == "--out":
                suggested_output_path = command.get('suggested_output_file_path', '')
                if suggested_output_path:
                    extension_ts_content.append(f"{INDENT*2}const outputPathSuggestion = getSuggestedOutputPath('{suggested_output_path}');")
                output_prompt = f"{INDENT*2}const outputPath = await vscode.window.showSaveDialog({{ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output' }});"
                args_str += f" --out ${{outputPath.fsPath}}"
            
        for prompt in command.get('prompts', []):
            prompt_var_name = prompt['name'][2:].replace('-', '_').lower()
            promptMessage = prompt.get('message', f"Enter the value for {prompt['name']}").replace("'", "\\'")
            choices = prompt.get('choices', [])
            arg = next((a for a in command['args'] if a['name'] == prompt['name']), None)
            
            if choices:
                choices_str = ', '.join([f"'{c}'" for c in choices])        
                extension_ts_content.append(f"{INDENT*2}const {prompt_var_name} = await vscode.window.showQuickPick( " + \
                    f"[{choices_str}], {{ title: '{prompt['message']}'}} );")
            else:   
                if arg.get('type') == 'bool':
                    extension_ts_content.append(f"{INDENT*2}const {prompt_var_name} = await vscode.window.showQuickPick( " + \
                        "[ 'Yes', 'No' ], { title: 'Select Yes or No' } ) === 'Yes';")
                else: 
                    extension_ts_content.append(f"{INDENT*2}const {prompt_var_name} = " + \
                        f"await vscode.window.showInputBox({{ prompt: '{promptMessage}', " + \
                        f"value: '{prompt.get('default', '')}' " + \
                        f"}});")
            
            if arg.get('type') == 'bool':
                extension_ts_content.append(f"{INDENT*2}const {prompt_var_name}_arg = {prompt_var_name} ? '--{prompt['name']}' : '';")
                args_str += f" ${{{prompt_var_name}_arg}}"
            else:    
                args_str += f" {prompt['name']} ${{{prompt_var_name}}}"
                
        extension_ts_content.append(output_prompt)
        extension_ts_content.append(f"{INDENT}   if (!outputPath) return;")
        extension_ts_content.append(f"{INDENT}   const command = `avrotize {command['command']} {args_str}`;")
        extension_ts_content.append(f"{INDENT*2}exec(command, (error, stdout, stderr) => {{")
        extension_ts_content.append(f"{INDENT*3}if (error) {{")
        extension_ts_content.append(f"{INDENT*4}vscode.window.showErrorMessage(`Error: ${{stderr}}`);")
        extension_ts_content.append(f"{INDENT*3}}} else {{")
        extension_ts_content.append(f"{INDENT*4}vscode.window.showInformationMessage(`Success: ${{stdout}}`);")
        extension_ts_content.append(f"{INDENT*3}}}")
        extension_ts_content.append(f"{INDENT*2}}});")
        extension_ts_content.append(f"{INDENT}}}));")
        extension_ts_content.append("")

    # Finalize the src/extension.ts content
    extension_ts_content.append(f"{INDENT}context.subscriptions.push(...disposables);")
    extension_ts_content.append("}")
    extension_ts_content.append("")
    extension_ts_content.append("export function deactivate() {}")
    extension_ts_content.append("\nfunction getSuggestedOutputPath(suggestedOutputPath: string) {")
    extension_ts_content.append(f"{INDENT}const activeEditor = vscode.window.activeTextEditor;")
    extension_ts_content.append(f"{INDENT}const inputFilePath = activeEditor?.document.uri.fsPath;")
    extension_ts_content.append(f"{INDENT}const inputFileName = inputFilePath ? path.basename(inputFilePath) : '';")
    extension_ts_content.append(f"{INDENT}return suggestedOutputPath.replace('{{input_file_name}}', inputFileName);")
    extension_ts_content.append("}")
    

    # Write the src/extension.ts content to file
    with open(extension_ts_path, 'w', encoding='utf-8') as file:
        file.write("\n".join(extension_ts_content))

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
