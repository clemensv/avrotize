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
        if latest_tag.startswith('v'):
            latest_tag = latest_tag[1:]
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
        command_description = clip_command_description(command_description)
        all_extensions.update(command['extensions'])
        ext_conditions = " || ".join([f"resourceExtname == {ext}" for ext in command['extensions']])
        convert_submenus.append({
            "command": f"avrotize.{command['command']}",
            "group": command['group'],
            "when": ext_conditions,
            "title": command_description
        })
        
    # sort the submenus by title
    convert_submenus = sorted(convert_submenus, key=lambda x: x['title'])
    
    package_json['contributes']['menus']['editor/context'].append({
        "submenu": "convertSubmenu",
        "title": "Convert",
        "group": "navigation",
        "when": " || ".join([f"resourceExtname == {ext}" for ext in all_extensions])
    })
    package_json['contributes']['menus']['convertSubmenu'] = convert_submenus
    package_json['contributes']['submenus'] = [{
                "id": "convertSubmenu",
                "label": "Convert to"
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
        "import * as fs from 'fs';",
        "",
        f"const currentVersionMajor = {latest_version.split('.',2)[0]};",
        f"const currentVersionMinor = {latest_version.split('.',2)[1]};",
        f"const currentVersionPatch = {latest_version.split('.',2)[2]};",
        "const avrotizeInstallSpec = 'avrotize[mcp]';",
        "export const mcpProviderId = 'avrotize.local-mcp';",
        "",
        "type McpServerProvider = {",
        "    provideMcpServerDefinitions: () => unknown[];",
        "    resolveMcpServerDefinition?: (server: unknown) => unknown;",
        "};",
        "",
        "type McpStdioServerDefinitionConstructor = new (",
        "    label: string,",
        "    command: string,",
        "    args?: string[],",
        "    env?: Record<string, string | number | null>,",
        "    version?: string",
        ") => unknown;",
        "",
        "export function createAvrotizeMcpServerDefinitionProvider(",
        "    mcpStdioServerDefinition: McpStdioServerDefinitionConstructor,",
        "    version: string",
        "): McpServerProvider {",
        "    return {",
        "        provideMcpServerDefinitions: () => [",
        "            new mcpStdioServerDefinition(",
        "                'Avrotize MCP',",
        "                'avrotize',",
        "                ['mcp'],",
        "                undefined,",
        "                version",
        "            )",
        "        ],",
        "        resolveMcpServerDefinition: (server) => server",
        "    };",
        "}",
     ]
    
    extension_ts_content.append(f"async function checkAvrotizeTool(context: vscode.ExtensionContext, outputChannel: vscode.OutputChannel): Promise<boolean> {{")
    extension_ts_content.append(f"{INDENT}try {{")
    extension_ts_content.append(f"{INDENT*2}const toolAvailable = await execShellCommand('avrotize --version')")
    extension_ts_content.append(f"{INDENT*3}.then(async (output:string) => {{");
    extension_ts_content.append(f"{INDENT*4}const version = output.trim().split(' ')[1];")
    extension_ts_content.append(f"{INDENT*4}const [major, minor, patch] = version.split('.',3).map(num => parseInt(num));")
    extension_ts_content.append(f"{INDENT*4}outputChannel.appendLine(`avrotize tool version: ${{version}}`);")
    extension_ts_content.append(f"{INDENT*4}if (major < currentVersionMajor || (major === currentVersionMajor && minor < currentVersionMinor) || (major === currentVersionMajor && minor === currentVersionMinor && patch < currentVersionPatch)) {{")
    extension_ts_content.append(f"{INDENT*5}outputChannel.show(true);")
    extension_ts_content.append(f"{INDENT*5}outputChannel.appendLine('avrotize tool version is outdated. Updating.');")
    extension_ts_content.append(f"{INDENT*5}await execShellCommand(`pip install --upgrade \"${{avrotizeInstallSpec}}\"`, outputChannel);")
    extension_ts_content.append(f"{INDENT*5}vscode.window.showInformationMessage('avrotize tool has been updated successfully.');")
    extension_ts_content.append(f"{INDENT*4}}};")
    extension_ts_content.append(f"{INDENT*4}return true;")
    extension_ts_content.append(f"{INDENT*3}}})")
    extension_ts_content.append(f"{INDENT*3}.catch(async (error) => {{")
    extension_ts_content.append(f"{INDENT*4}const installOption = await vscode.window.showWarningMessage(")
    extension_ts_content.append(f"{INDENT*5}'avrotize tool is not available. Do you want to install it?', 'Yes', 'No');")
    extension_ts_content.append(f"{INDENT*4}if (installOption === 'Yes') {{")
    extension_ts_content.append(f"{INDENT*5}if (!await isPythonAvailable()) {{")
    extension_ts_content.append(f"{INDENT*6}const downloadOption = await vscode.window.showErrorMessage('Python 3.10 or higher must be installed. Do you want to open the download page?', 'Yes', 'No');")
    extension_ts_content.append(f"{INDENT*6}if (downloadOption === 'Yes') {{")
    extension_ts_content.append(f"{INDENT*7}vscode.env.openExternal(vscode.Uri.parse('https://www.python.org/downloads/'));")
    extension_ts_content.append(f"{INDENT*6}}}")
    extension_ts_content.append(f"{INDENT*6}return false;")
    extension_ts_content.append(f"{INDENT*5}}}")
    extension_ts_content.append(f"{INDENT*5}outputChannel.show(true);")
    extension_ts_content.append(f"{INDENT*5}outputChannel.appendLine('Installing avrotize tool...');")
    extension_ts_content.append(f"{INDENT*5}await execShellCommand(`pip install \"${{avrotizeInstallSpec}}\"`, outputChannel);")
    extension_ts_content.append(f"{INDENT*5}vscode.window.showInformationMessage('avrotize tool has been installed successfully.');")
    extension_ts_content.append(f"{INDENT*5}return true;")
    extension_ts_content.append(f"{INDENT*4}}}")
    extension_ts_content.append(f"{INDENT*4}return false;")
    extension_ts_content.append(f"{INDENT*3}}});")
    extension_ts_content.append(f"{INDENT*2}return toolAvailable;")
    extension_ts_content.append(f"{INDENT}}} catch (error) {{")
    extension_ts_content.append(f"{INDENT*2}vscode.window.showErrorMessage('Error checking avrotize tool availability: ' + error);")
    extension_ts_content.append(f"{INDENT*2}return false;")
    extension_ts_content.append(f"{INDENT}}}")
    extension_ts_content.append("}")
    
    extension_ts_content.append(f"async function isPythonAvailable(): Promise<boolean> {{")
    extension_ts_content.append(f"{INDENT}try {{")
    extension_ts_content.append(f"{INDENT*2}const output = await execShellCommand('python --version');")
    extension_ts_content.append(f"{INDENT*2}const version = output.trim().split(' ')[1];")
    extension_ts_content.append(f"{INDENT*2}const [major, minor] = version.split('.').map(num => parseInt(num));")
    # show info message if python version is less than 3.10
    extension_ts_content.append(f"{INDENT*2}if (major < 3 || (major === 3 && minor < 10)) {{")
    extension_ts_content.append(f"{INDENT*3}vscode.window.showInformationMessage('Python 3.10 or higher must be installed. Found version: ' + version);")
    extension_ts_content.append(f"{INDENT*3}return false;")
    extension_ts_content.append(f"{INDENT*2}}}")
    extension_ts_content.append(f"{INDENT*2}return major === 3 && minor >= 11;")
    extension_ts_content.append(f"{INDENT}}} catch {{")
    extension_ts_content.append(f"{INDENT*2}return false;")
    extension_ts_content.append(f"{INDENT}}}")
    extension_ts_content.append("}")

    extension_ts_content.append(f"\nfunction execShellCommand(cmd: string, outputChannel?: vscode.OutputChannel): Promise<string> {{")
    extension_ts_content.append(f"{INDENT}return new Promise((resolve, reject) => {{")
    extension_ts_content.append(f"{INDENT*2}const process = exec(cmd, (error, stdout, stderr) => {{")
    extension_ts_content.append(f"{INDENT*3}if (error) {{")
    extension_ts_content.append(f"{INDENT*4}reject(error);")
    extension_ts_content.append(f"{INDENT*3}}} else {{")
    extension_ts_content.append(f"{INDENT*4}resolve(stdout ? stdout : stderr);")
    extension_ts_content.append(f"{INDENT*3}}}")
    extension_ts_content.append(f"{INDENT*2}}});")
    extension_ts_content.append(f"{INDENT*2}if (outputChannel) {{")
    extension_ts_content.append(f"{INDENT*3}process.stdout?.on('data', (data) => {{")
    extension_ts_content.append(f"{INDENT*4}outputChannel.append(data.toString());")
    extension_ts_content.append(f"{INDENT*3}}});")
    extension_ts_content.append(f"{INDENT*3}process.stderr?.on('data', (data) => {{")
    extension_ts_content.append(f"{INDENT*4}outputChannel.append(data.toString());")
    extension_ts_content.append(f"{INDENT*3}}});")
    extension_ts_content.append(f"{INDENT*2}}}")
    extension_ts_content.append(f"{INDENT}}});")
    extension_ts_content.append("}")    
    
    extension_ts_content.append(f"function executeCommand(command: string, outputPath: vscode.Uri | null, outputChannel: vscode.OutputChannel) {{")
    extension_ts_content.append(f"{INDENT}exec(command, (error, stdout, stderr) => {{")
    extension_ts_content.append(f"{INDENT*2}if (error) {{")
    extension_ts_content.append(f"{INDENT*3}outputChannel.appendLine(`Error: ${{error.message}}`);")
    extension_ts_content.append(f"{INDENT*3}vscode.window.showErrorMessage(`Error: ${{stderr}}`);")
    extension_ts_content.append(f"{INDENT*2}}} else {{")
    extension_ts_content.append(f"{INDENT*3}outputChannel.appendLine(stdout);")
    extension_ts_content.append(f"{INDENT*3}if (outputPath) {{")
    extension_ts_content.append(f"{INDENT*4}if (fs.existsSync(outputPath.fsPath)) {{")
    extension_ts_content.append(f"{INDENT*5}const stats = fs.statSync(outputPath.fsPath);")
    extension_ts_content.append(f"{INDENT*5}if (stats.isFile()) {{")
    extension_ts_content.append(f"{INDENT*6}vscode.workspace.openTextDocument(outputPath).then((document) => {{")
    extension_ts_content.append(f"{INDENT*7}vscode.window.showTextDocument(document);")
    extension_ts_content.append(f"{INDENT*6}}});")
    extension_ts_content.append(f"{INDENT*5}}} else if (stats.isDirectory()) {{")
    extension_ts_content.append(f"{INDENT*6}vscode.commands.executeCommand('vscode.openFolder', vscode.Uri.file(outputPath.fsPath), true);")
    extension_ts_content.append(f"{INDENT*5}}}")
    extension_ts_content.append(f"{INDENT*4}}}")
    extension_ts_content.append(f"{INDENT*3}}} else {{")
    extension_ts_content.append(f"{INDENT*4}vscode.workspace.openTextDocument({{ content: stdout }}).then((document) => {{")
    extension_ts_content.append(f"{INDENT*5}vscode.window.showTextDocument(document);")
    extension_ts_content.append(f"{INDENT*4}}});")
    extension_ts_content.append(f"{INDENT*3}}}")
    extension_ts_content.append(f"{INDENT*3}vscode.window.showInformationMessage(`Success: ${{stdout}}`);")
    extension_ts_content.append(f"{INDENT*2}}}")
    extension_ts_content.append(f"{INDENT}}});")
    extension_ts_content.append("}")

    
    extension_ts_content.extend(
        [
            "export function activate(context: vscode.ExtensionContext) {",
            f"{INDENT}const disposables: vscode.Disposable[] = [];",
            f"{INDENT}(async () => {{",
            f"{INDENT*2}const outputChannel = vscode.window.createOutputChannel('avrotize');",
            f"{INDENT*2}const vscodeWithMcp = vscode as typeof vscode & {{",
            f"{INDENT*3}lm?: {{",
            f"{INDENT*4}registerMcpServerDefinitionProvider: (id: string, provider: {{",
            f"{INDENT*5}provideMcpServerDefinitions: () => unknown[];",
            f"{INDENT*5}resolveMcpServerDefinition?: (server: unknown) => unknown;",
            f"{INDENT*4}}}) => vscode.Disposable;",
            f"{INDENT*3}}};",
            f"{INDENT*3}McpStdioServerDefinition?: new (",
            f"{INDENT*4}label: string,",
            f"{INDENT*4}command: string,",
            f"{INDENT*4}args?: string[],",
            f"{INDENT*4}env?: Record<string, string | number | null>,",
            f"{INDENT*4}version?: string",
            f"{INDENT*3}) => unknown;",
            f"{INDENT*2}}};",
            "",
            f"{INDENT*2}if (vscodeWithMcp.lm?.registerMcpServerDefinitionProvider && vscodeWithMcp.McpStdioServerDefinition) {{",
            f"{INDENT*3}const mcpStdioServerDefinition = vscodeWithMcp.McpStdioServerDefinition;",
            f"{INDENT*3}disposables.push(vscodeWithMcp.lm.registerMcpServerDefinitionProvider(",
            f"{INDENT*4}mcpProviderId,",
            f"{INDENT*4}createAvrotizeMcpServerDefinitionProvider(",
            f"{INDENT*5}mcpStdioServerDefinition,",
            f"{INDENT*5}`${{currentVersionMajor}}.${{currentVersionMinor}}.${{currentVersionPatch}}`",
            f"{INDENT*4})",
            f"{INDENT*3}));",
            f"{INDENT*3}outputChannel.appendLine('Registered MCP server provider: Avrotize MCP');",
            f"{INDENT*2}}}",
            ""
        ]
    )

    # Add command implementations
    for command in commands:
        file_basename_defined = False
        extension_ts_content.append(f"{INDENT*2}disposables.push(vscode.commands.registerCommand('avrotize.{command['command']}', async (uri: vscode.Uri) => {{")
        extension_ts_content.append(f"{INDENT*3}if (!await checkAvrotizeTool(context, outputChannel)) {{ return; }}")
        extension_ts_content.append(f"{INDENT*3}const filePath = uri.fsPath;")
        
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
                    extension_ts_content.append(f"{INDENT*3}const outputPathSuggestion = getSuggestedOutputPath(filePath, '{suggested_output_path}');")
                output_prompt = f"{INDENT*3}const outputPath = await vscode.window.showSaveDialog(" + \
                    f"{{ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : {{ {filter_extensions} }} }});"
                args_str += f" --out ${{outputPath.fsPath}}"
        
        for prompt in prompts:
            prompt_var_name = prompt['name'][2:].replace('-', '_').lower()+ '_value'
            prompt_message = prompt.get('message', f"Enter the value for {prompt['name']}").replace("'", "\\'")
            choices = prompt.get('choices', [])
            arg = next((a for a in command['args'] if a['name'] == prompt['name']), None)
            
            if choices:
                choices_str = ', '.join([f"'{c}'" for c in choices])
                extension_ts_content.append(f"{INDENT*3}const {prompt_var_name} = await vscode.window.showQuickPick([{choices_str}], {{ placeHolder: '{prompt_message}' }});")
            else:
                if arg.get('type') == 'bool':
                    extension_ts_content.append(f"{INDENT*3}const {prompt_var_name} = await vscode.window.showQuickPick(['Yes', 'No'], {{ title: '{prompt_message}' }}) === 'Yes';")
                else:
                    default_value = prompt.get('default', '')
                    if isinstance(default_value, bool):
                        extension_ts_content.append(f"{INDENT*3}const {prompt_var_name}_default_value = {'Yes' if default_value else 'No'};")
                    elif default_value:
                        if not file_basename_defined:
                            extension_ts_content.append(f"{INDENT*3}const fileBaseName = path.basename(filePath, path.extname(filePath));")
                            file_basename_defined = True
                        extension_ts_content.append(f"{INDENT*3}const {prompt_var_name}_default_value = '{default_value}'.replace('{{input_file_name}}', fileBaseName);")
                    line = f"{INDENT*3}const {prompt_var_name} = await vscode.window.showInputBox({{ prompt: '{prompt_message}'" 
                    if default_value:
                        line += f", value: `${{ {prompt_var_name}_default_value }}` }});"
                    else:
                        line += " });"
                    extension_ts_content.append(line)
            
            if arg.get('type') == 'bool':
                extension_ts_content.append(f"{INDENT*3}const {prompt_var_name}_arg = {prompt_var_name} ? '{prompt['name']}' : '';")
                args_str += f" ${{{prompt_var_name}_arg}}"
            else:
                extension_ts_content.append(f"{INDENT*3}const {prompt_var_name}_arg = {prompt_var_name} ? `{prompt['name']} ${{{prompt_var_name}}}` : '';")
                args_str += f" ${{{prompt_var_name}_arg}}"
                
        if output_prompt:
            extension_ts_content.append(output_prompt)
            extension_ts_content.append(f"{INDENT*3}if (!outputPath) {{ return; }}")
        extension_ts_content.append(f"{INDENT*3}const command = `avrotize {command['command']} {args_str}`;")
        extension_ts_content.append(f"{INDENT*3}executeCommand(command, {'outputPath' if output_prompt else 'null'}, outputChannel);")
        extension_ts_content.append(f"{INDENT*2}}}));")
        extension_ts_content.append("")

    # Finalize the src/extension.ts content
    extension_ts_content.append(f"{INDENT*2}context.subscriptions.push(...disposables);")
    extension_ts_content.append(f"{INDENT}}})();")
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
        command_description = command_description[command_description.find(' to ')+4:]
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
