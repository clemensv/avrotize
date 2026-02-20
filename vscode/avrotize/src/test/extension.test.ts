import * as assert from 'assert';
import * as vscode from 'vscode';
import { createAvrotizeMcpServerDefinitionProvider, mcpProviderId } from '../extension';

class FakeMcpStdioServerDefinition {
    label: string;
    command: string;
    args?: string[];
    env?: Record<string, string | number | null>;
    version?: string;

    constructor(
        label: string,
        command: string,
        args?: string[],
        env?: Record<string, string | number | null>,
        version?: string
    ) {
        this.label = label;
        this.command = command;
        this.args = args;
        this.env = env;
        this.version = version;
    }
}

suite('Extension Test Suite', () => {
	vscode.window.showInformationMessage('Start extension tests.');

	test('Contributes MCP provider metadata', async () => {
		const extension = vscode.extensions.getExtension('clemensvasters.avrotize');
		assert.ok(extension, 'Extension clemensvasters.avrotize should be available');
		await extension?.activate();
		const packageJson = extension?.packageJSON as {
			activationEvents?: string[];
			contributes?: {
				mcpServerDefinitionProviders?: Array<{ id: string; label: string }>;
			};
		};

		assert.ok(packageJson.activationEvents?.includes('onStartupFinished'), 'Extension should activate on startup to register MCP provider');

		const providers = packageJson.contributes?.mcpServerDefinitionProviders ?? [];
		assert.ok(providers.some((provider) => provider.id === mcpProviderId && provider.label === 'Avrotize MCP'),
			'Extension should contribute Avrotize MCP provider metadata');
	});

	test('MCP provider returns avrotize mcp stdio definition', () => {
		const provider = createAvrotizeMcpServerDefinitionProvider(FakeMcpStdioServerDefinition, '2.1.3');
		const definitions = provider.provideMcpServerDefinitions() as FakeMcpStdioServerDefinition[];

		assert.strictEqual(definitions.length, 1, 'Provider should return one MCP server definition');
		assert.strictEqual(definitions[0].label, 'Avrotize MCP');
		assert.strictEqual(definitions[0].command, 'avrotize');
		assert.deepStrictEqual(definitions[0].args, ['mcp']);
		assert.strictEqual(definitions[0].version, '2.1.3');
	});

	test('MCP provider resolve returns original server', () => {
		const provider = createAvrotizeMcpServerDefinitionProvider(FakeMcpStdioServerDefinition, '2.1.3');
		const server = { name: 'server' };
		const resolved = provider.resolveMcpServerDefinition?.(server);
		assert.strictEqual(resolved, server);
	});
});
