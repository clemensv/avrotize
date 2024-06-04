import * as vscode from 'vscode';
import { exec } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';

const currentVersionMajor = 2;
const currentVersionMinor = 0;
const currentVersionPatch = 3;
async function checkAvrotizeTool(context: vscode.ExtensionContext, outputChannel: vscode.OutputChannel): Promise<boolean> {
    try {
        const toolAvailable = await execShellCommand('avrotize --version')
            .then(async (output:string) => {
                const version = output.trim().split(' ')[1];
                const [major, minor, patch] = version.split('.',3).map(num => parseInt(num));
                outputChannel.appendLine(`avrotize tool version: ${version}`);
                if (major < currentVersionMajor || (major === currentVersionMajor && minor < currentVersionMinor) || (major === currentVersionMajor && minor === currentVersionMinor && patch < currentVersionPatch)) {
                    outputChannel.show(true);
                    outputChannel.appendLine('avrotize tool version is outdated. Updating.');
                    await execShellCommand('pip install --upgrade avrotize', outputChannel);
                    vscode.window.showInformationMessage('avrotize tool has been updated successfully.');
                };
                return true;
            })
            .catch(async (error) => {
                const installOption = await vscode.window.showWarningMessage(
                    'avrotize tool is not available. Do you want to install it?', 'Yes', 'No');
                if (installOption === 'Yes') {
                    if (!await isPythonAvailable()) {
                        const downloadOption = await vscode.window.showErrorMessage('Python 3.11 or higher must be installed. Do you want to open the download page?', 'Yes', 'No');
                        if (downloadOption === 'Yes') {
                            vscode.env.openExternal(vscode.Uri.parse('https://www.python.org/downloads/'));
                        }
                        return false;
                    }
                    outputChannel.show(true);
                    outputChannel.appendLine('Installing avrotize tool...');
                    await execShellCommand('pip install avrotize', outputChannel);
                    vscode.window.showInformationMessage('avrotize tool has been installed successfully.');
                    return true;
                }
                return false;
            });
        return toolAvailable;
    } catch (error) {
        vscode.window.showErrorMessage('Error checking avrotize tool availability: ' + error);
        return false;
    }
}
async function isPythonAvailable(): Promise<boolean> {
    try {
        const output = await execShellCommand('python --version');
        const version = output.trim().split(' ')[1];
        const [major, minor] = version.split('.').map(num => parseInt(num));
        if (major < 3 || (major === 3 && minor < 11)) {
            vscode.window.showInformationMessage('Python 3.11 or higher must be installed. Found version: ' + version);
            return false;
        }
        return major === 3 && minor >= 11;
    } catch {
        return false;
    }
}

function execShellCommand(cmd: string, outputChannel?: vscode.OutputChannel): Promise<string> {
    return new Promise((resolve, reject) => {
        const process = exec(cmd, (error, stdout, stderr) => {
            if (error) {
                reject(error);
            } else {
                resolve(stdout ? stdout : stderr);
            }
        });
        if (outputChannel) {
            process.stdout?.on('data', (data) => {
                outputChannel.append(data.toString());
            });
            process.stderr?.on('data', (data) => {
                outputChannel.append(data.toString());
            });
        }
    });
}
function executeCommand(command: string, outputPath: vscode.Uri | null, outputChannel: vscode.OutputChannel) {
    exec(command, (error, stdout, stderr) => {
        if (error) {
            outputChannel.appendLine(`Error: ${error.message}`);
            vscode.window.showErrorMessage(`Error: ${stderr}`);
        } else {
            outputChannel.appendLine(stdout);
            if (outputPath) {
                if (fs.existsSync(outputPath.fsPath)) {
                    const stats = fs.statSync(outputPath.fsPath);
                    if (stats.isFile()) {
                        vscode.workspace.openTextDocument(outputPath).then((document) => {
                            vscode.window.showTextDocument(document);
                        });
                    } else if (stats.isDirectory()) {
                        vscode.commands.executeCommand('vscode.openFolder', vscode.Uri.file(outputPath.fsPath), true);
                    }
                }
            } else {
                vscode.workspace.openTextDocument({ content: stdout }).then((document) => {
                    vscode.window.showTextDocument(document);
                });
            }
            vscode.window.showInformationMessage(`Success: ${stdout}`);
        }
    });
}
export function activate(context: vscode.ExtensionContext) {
    const disposables: vscode.Disposable[] = [];
    (async () => {
        const outputChannel = vscode.window.createOutputChannel('avrotize');

        disposables.push(vscode.commands.registerCommand('avrotize.p2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize p2a ${filePath} --out ${outputPath.fsPath}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2p', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-proto');
            const naming_value = await vscode.window.showQuickPick(['snake', 'camel', 'pascal'], { placeHolder: 'Select type naming convention' });
            const naming_value_arg = naming_value ? `--naming ${naming_value}` : '';
            const allow_optional_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Enable support for \'optional\' fields?' }) === 'Yes';
            const allow_optional_value_arg = allow_optional_value ? '--allow-optional' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2p ${filePath} --out ${outputPath.fsPath} ${naming_value_arg} ${allow_optional_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.j2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the namespace for the Avro schema' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const split_top_level_records_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Split top-level records into separate files?' }) === 'Yes';
            const split_top_level_records_value_arg = split_top_level_records_value ? '--split-top-level-records' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize j2a ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg} ${split_top_level_records_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2j', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.jsons');
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'jsons File': ['jsons'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2j ${filePath} --out ${outputPath.fsPath}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.x2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the namespace for the Avro schema' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize x2a ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2x', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.xsd');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the target namespace for the XSD schema (optional)' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'xsd File': ['xsd'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2x ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2k', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.kql');
            const kusto_uri_value = await vscode.window.showInputBox({ prompt: 'Enter the Kusto Cluster URI (optional)' });
            const kusto_uri_value_arg = kusto_uri_value ? `--kusto-uri ${kusto_uri_value}` : '';
            const kusto_database_value = await vscode.window.showInputBox({ prompt: 'Enter the Kusto database name (optional)' });
            const kusto_database_value_arg = kusto_database_value ? `--kusto-database ${kusto_database_value}` : '';
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Kusto table?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const emit_cloudevents_dispatch_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Emit a _cloudevents_dispatch ingestion table and update policies?' }) === 'Yes';
            const emit_cloudevents_dispatch_value_arg = emit_cloudevents_dispatch_value ? '--emit-cloudevents-dispatch' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'kql File': ['kql'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2k ${filePath} --out ${outputPath.fsPath} ${kusto_uri_value_arg} ${kusto_database_value_arg} ${emit_cloudevents_columns_value_arg} ${emit_cloudevents_dispatch_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.k2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{kusto_database}.avsc');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the namespace for the Avro schema' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const emit_cloudevents_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Emit CloudEvents declarations for each table?' }) === 'Yes';
            const emit_cloudevents_value_arg = emit_cloudevents_value ? '--emit-cloudevents' : '';
            const emit_xregistry_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Emit an xRegistry manifest with CloudEvents declarations?' }) === 'Yes';
            const emit_xregistry_value_arg = emit_xregistry_value ? '--emit-xregistry' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize k2a ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg} ${emit_cloudevents_value_arg} ${emit_xregistry_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2sql', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.sql');
            const dialect_value = await vscode.window.showQuickPick(['mysql', 'mariadb', 'postgres', 'sqlserver', 'oracle', 'sqlite', 'bigquery', 'snowflake', 'redshift', 'db2'], { placeHolder: 'Select the SQL dialect' });
            const dialect_value_arg = dialect_value ? `--dialect ${dialect_value}` : '';
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the SQL table?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'sql File': ['sql'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2sql ${filePath} --out ${outputPath.fsPath} ${dialect_value_arg} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2mongo', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the MongoDB schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2mongo ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2pq', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.parquet');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Parquet file?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'parquet File': ['parquet'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2pq ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2ib', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.iceberg');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Iceberg schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'iceberg File': ['iceberg'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2ib ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.pq2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the namespace for the Avro schema' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize pq2a ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.asn2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize asn2a ${filePath} --out ${outputPath.fsPath}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.kstruct2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize kstruct2a ${filePath} --out ${outputPath.fsPath}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2cs', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-cs');
            const fileBaseName = path.basename(filePath, path.extname(filePath));
            const namespace_value_default_value = '{input_file_name}-cs'.replace('{input_file_name}', fileBaseName);
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the C# root namespace for the project', value: `${ namespace_value_default_value }` });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const system_text_json_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use System.Text.Json annotations?' }) === 'Yes';
            const system_text_json_annotation_value_arg = system_text_json_annotation_value ? '--system_text_json_annotation' : '';
            const pascal_properties_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use PascalCase properties?' }) === 'Yes';
            const pascal_properties_value_arg = pascal_properties_value ? '--pascal-properties' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2cs ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg} ${avro_annotation_value_arg} ${system_text_json_annotation_value_arg} ${pascal_properties_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2java', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-java');
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const jackson_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Jackson annotations?' }) === 'Yes';
            const jackson_annotation_value_arg = jackson_annotation_value ? '--jackson-annotation' : '';
            const pascal_properties_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use PascalCase properties?' }) === 'Yes';
            const pascal_properties_value_arg = pascal_properties_value ? '--pascal-properties' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2java ${filePath} --out ${outputPath.fsPath} ${avro_annotation_value_arg} ${jackson_annotation_value_arg} ${pascal_properties_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2py', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-py');
            const dataclasses_json_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use dataclasses-json annotations?' }) === 'Yes';
            const dataclasses_json_annotation_value_arg = dataclasses_json_annotation_value ? '--dataclasses-json-annotation' : '';
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use apache_avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2py ${filePath} --out ${outputPath.fsPath} ${dataclasses_json_annotation_value_arg} ${avro_annotation_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2ts', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-ts');
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const typedjson_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use TypedJSON annotations?' }) === 'Yes';
            const typedjson_annotation_value_arg = typedjson_annotation_value ? '--typedjson-annotation' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2ts ${filePath} --out ${outputPath.fsPath} ${avro_annotation_value_arg} ${typedjson_annotation_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2js', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-js');
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2js ${filePath} --out ${outputPath.fsPath} ${avro_annotation_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2cpp', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-cpp');
            const fileBaseName = path.basename(filePath, path.extname(filePath));
            const namespace_value_default_value = '{input_file_name}'.replace('{input_file_name}', fileBaseName);
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the root namespace for the C++ classes (optional)', value: `${ namespace_value_default_value }` });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const json_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use JSON annotations?' }) === 'Yes';
            const json_annotation_value_arg = json_annotation_value ? '--json-annotation' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2cpp ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg} ${avro_annotation_value_arg} ${json_annotation_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2go', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-go');
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const json_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use JSON annotations?' }) === 'Yes';
            const json_annotation_value_arg = json_annotation_value ? '--json-annotation' : '';
            const package_value = await vscode.window.showInputBox({ prompt: 'Enter the Go package name (optional)' });
            const package_value_arg = package_value ? `--package ${package_value}` : '';
            const package_site_value = await vscode.window.showInputBox({ prompt: 'Enter the Go package site (optional)' });
            const package_site_value_arg = package_site_value ? `--package-site ${package_site_value}` : '';
            const package_username_value = await vscode.window.showInputBox({ prompt: 'Enter the Go package username (optional)' });
            const package_username_value_arg = package_username_value ? `--package-username ${package_username_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2go ${filePath} --out ${outputPath.fsPath} ${avro_annotation_value_arg} ${json_annotation_value_arg} ${package_value_arg} ${package_site_value_arg} ${package_username_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2rust', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}-rust');
            const package_value = await vscode.window.showInputBox({ prompt: 'Enter the Rust package name (optional)' });
            const package_value_arg = package_value ? `--package ${package_value}` : '';
            const avro_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use Avro annotations?' }) === 'Yes';
            const avro_annotation_value_arg = avro_annotation_value ? '--avro-annotation' : '';
            const json_annotation_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Use JSON annotations?' }) === 'Yes';
            const json_annotation_value_arg = json_annotation_value ? '--json-annotation' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'All Files': ['*'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2rust ${filePath} --out ${outputPath.fsPath} ${package_value_arg} ${avro_annotation_value_arg} ${json_annotation_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2dp', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const record_type_value = await vscode.window.showInputBox({ prompt: 'Enter the record type in the Avro schema (optional)' });
            const record_type_value_arg = record_type_value ? `--record-type ${record_type_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2dp ${filePath} --out ${outputPath.fsPath} ${record_type_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2md', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.md');
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'md File': ['md'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2md ${filePath} --out ${outputPath.fsPath}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.pcf', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const command = `avrotize pcf ${filePath}`;
            executeCommand(command, null, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.csv2a', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.avsc');
            const namespace_value = await vscode.window.showInputBox({ prompt: 'Enter the namespace for the Avro schema' });
            const namespace_value_arg = namespace_value ? `--namespace ${namespace_value}` : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'avsc File': ['avsc'] } });
            if (!outputPath) { return; }
            const command = `avrotize csv2a ${filePath} --out ${outputPath.fsPath} ${namespace_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2cassandra', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.cql');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Cassandra schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'cql File': ['cql'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2cassandra ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2dynamodb', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the DynamoDB schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2dynamodb ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2es', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Elasticsearch schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2es ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2couchdb', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the CouchDB schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2couchdb ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2neo4j', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.cypher');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Neo4j schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'cypher File': ['cypher'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2neo4j ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2firebase', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the Firebase schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2firebase ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2cosmos', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the CosmosDB schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2cosmos ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        disposables.push(vscode.commands.registerCommand('avrotize.a2hbase', async (uri: vscode.Uri) => {
            if (!await checkAvrotizeTool(context, outputChannel)) { return; }
            const filePath = uri.fsPath;
            const outputPathSuggestion = getSuggestedOutputPath(filePath, '{input_file_name}.json');
            const emit_cloudevents_columns_value = await vscode.window.showQuickPick(['Yes', 'No'], { title: 'Add CloudEvents columns to the HBase schema?' }) === 'Yes';
            const emit_cloudevents_columns_value_arg = emit_cloudevents_columns_value ? '--emit-cloudevents-columns' : '';
            const outputPath = await vscode.window.showSaveDialog({ defaultUri: vscode.Uri.file(outputPathSuggestion), saveLabel: 'Save Output', filters : { 'json File': ['json'] } });
            if (!outputPath) { return; }
            const command = `avrotize a2hbase ${filePath} --out ${outputPath.fsPath} ${emit_cloudevents_columns_value_arg}`;
            executeCommand(command, outputPath, outputChannel);
        }));

        context.subscriptions.push(...disposables);
    })();
}

export function deactivate() {}

function getSuggestedOutputPath(inputFilePath: string, suggestedOutputPath: string) {
    const inputFileName = inputFilePath ? path.basename(inputFilePath, path.extname(inputFilePath)) : '';
    const outFileName = suggestedOutputPath.replace('{input_file_name}', inputFileName);
    return path.join(path.dirname(inputFilePath), outFileName);
}