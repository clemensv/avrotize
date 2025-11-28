// =====================================================
// Avrotize & Structurize - Main JavaScript
// =====================================================

document.addEventListener('DOMContentLoaded', function() {
  initNavigation();
  initGallery();
});

// -----------------------------------------------------
// Navigation
// -----------------------------------------------------

function initNavigation() {
  const navToggle = document.querySelector('.nav-toggle');
  const navLinks = document.querySelector('.nav-links');
  
  if (navToggle && navLinks) {
    navToggle.addEventListener('click', function() {
      navLinks.classList.toggle('open');
    });
  }
}

// -----------------------------------------------------
// Gallery Functionality
// -----------------------------------------------------

function initGallery() {
  const fileTree = document.querySelector('.file-tree');
  if (!fileTree) return;
  
  // Handle file tree clicks
  fileTree.addEventListener('click', function(e) {
    const item = e.target.closest('.tree-item');
    if (!item) return;
    
    const filePath = item.dataset.path;
    const isFolder = item.classList.contains('folder');
    
    if (isFolder) {
      // Toggle folder expansion
      const children = item.nextElementSibling;
      if (children && children.classList.contains('tree-children')) {
        children.classList.toggle('collapsed');
        item.classList.toggle('expanded');
      }
    } else if (filePath) {
      // Load file content
      loadFileContent(filePath);
      
      // Update active state
      document.querySelectorAll('.tree-item.active').forEach(el => el.classList.remove('active'));
      item.classList.add('active');
    }
  });
}

async function loadFileContent(filePath) {
  const outputPanel = document.querySelector('.output-panel .panel-content');
  if (!outputPanel) return;
  
  try {
    const response = await fetch(filePath);
    if (!response.ok) throw new Error('Failed to load file');
    
    const content = await response.text();
    const extension = filePath.split('.').pop().toLowerCase();
    const language = getLanguageFromExtension(extension);
    
    outputPanel.innerHTML = `<pre class="line-numbers"><code class="language-${language}">${escapeHtml(content)}</code></pre>`;
    
    // Re-run Prism highlighting
    if (window.Prism) {
      Prism.highlightAllUnder(outputPanel);
    }
  } catch (error) {
    outputPanel.innerHTML = `<div class="error-message">Failed to load file: ${escapeHtml(filePath)}</div>`;
  }
}

function getLanguageFromExtension(ext) {
  const languageMap = {
    'json': 'json',
    'avsc': 'json',
    'py': 'python',
    'python': 'python',
    'cs': 'csharp',
    'java': 'java',
    'ts': 'typescript',
    'js': 'javascript',
    'go': 'go',
    'rs': 'rust',
    'cpp': 'cpp',
    'hpp': 'cpp',
    'h': 'c',
    'c': 'c',
    'proto': 'protobuf',
    'sql': 'sql',
    'xsd': 'xml',
    'xml': 'xml',
    'md': 'markdown',
    'yaml': 'yaml',
    'yml': 'yaml',
    'graphql': 'graphql',
    'gql': 'graphql'
  };
  return languageMap[ext] || 'plaintext';
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// -----------------------------------------------------
// File Tree Builder (used by gallery generation)
// -----------------------------------------------------

function buildFileTree(files, basePath) {
  const tree = {};
  
  files.forEach(file => {
    const relativePath = file.replace(basePath, '').replace(/^\//, '');
    const parts = relativePath.split('/');
    
    let current = tree;
    parts.forEach((part, index) => {
      if (index === parts.length - 1) {
        // It's a file
        current[part] = { type: 'file', path: file };
      } else {
        // It's a folder
        if (!current[part]) {
          current[part] = { type: 'folder', children: {} };
        }
        current = current[part].children;
      }
    });
  });
  
  return tree;
}

function renderFileTree(tree, container, basePath = '') {
  const sortedKeys = Object.keys(tree).sort((a, b) => {
    // Folders first, then files
    const aIsFolder = tree[a].type === 'folder';
    const bIsFolder = tree[b].type === 'folder';
    if (aIsFolder && !bIsFolder) return -1;
    if (!aIsFolder && bIsFolder) return 1;
    return a.localeCompare(b);
  });
  
  sortedKeys.forEach(key => {
    const item = tree[key];
    const fullPath = basePath ? `${basePath}/${key}` : key;
    
    if (item.type === 'folder') {
      const folderEl = document.createElement('div');
      folderEl.className = 'tree-item folder expanded';
      folderEl.innerHTML = `<span class="tree-icon">📁</span><span class="tree-name">${key}</span>`;
      container.appendChild(folderEl);
      
      const childrenEl = document.createElement('div');
      childrenEl.className = 'tree-children';
      container.appendChild(childrenEl);
      
      renderFileTree(item.children, childrenEl, fullPath);
    } else {
      const fileEl = document.createElement('div');
      fileEl.className = 'tree-item file';
      fileEl.dataset.path = item.path;
      
      const icon = getFileIcon(key);
      fileEl.innerHTML = `<span class="tree-icon">${icon}</span><span class="tree-name">${key}</span>`;
      container.appendChild(fileEl);
    }
  });
}

function getFileIcon(filename) {
  const ext = filename.split('.').pop().toLowerCase();
  const iconMap = {
    'json': '📄',
    'avsc': '📋',
    'py': '🐍',
    'cs': '💜',
    'java': '☕',
    'ts': '💙',
    'js': '💛',
    'go': '🔵',
    'rs': '🦀',
    'cpp': '⚙️',
    'proto': '📝',
    'sql': '🗃️',
    'xsd': '📐',
    'xml': '📐',
    'md': '📝',
    'graphql': '◼️'
  };
  return iconMap[ext] || '📄';
}
