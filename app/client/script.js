(function() {
    // Configuration
    const config = {
        chunkSize: 100000,
        maxSafeSize: 5 * 1024 * 1024,
        folders: ['coupon', 'market']
    };

    // State
    let state = {
        currentFile: null,
        fileContent: '',
        currentOffset: 0,
        folderData: new Map()
    };

    // Initialize application
    async function init() {
        await loadFolderContents();
        renderFolderTree();
    }

    // Load folder contents
    async function loadFolderContents() {
        try {
            for (const folder of config.folders) {
                const response = await fetch(`/api/data/${folder}`);
                if (!response.ok) throw new Error(`Failed to load ${folder} contents`);
                const files = await response.json();
                state.folderData.set(folder, files.map(f => f.name));
            }
        } catch (error) {
            console.error('Error loading folder contents:', error);
        }
    }

    // Render folder tree
    function renderFolderTree() {
        const root = document.getElementById('folder-root');
        const dataFolder = createTreeItem('data', 'folder');

        for (const [folder, files] of state.folderData) {
            const folderItem = createTreeItem(folder, 'folder');
            
            for (const file of files) {
                const fileItem = createTreeItem(file, 'file');
                fileItem.addEventListener('click', () => loadFile(folder, file));
                folderItem.querySelector('.tree-children').appendChild(fileItem);
            }

            dataFolder.querySelector('.tree-children').appendChild(folderItem);
        }

        root.appendChild(dataFolder);
    }

    // Create tree item element
    function createTreeItem(name, type) {
        const item = document.createElement('div');
        item.className = 'tree-item';
        
        const content = document.createElement('div');
        content.className = 'tree-content';
        
        const icon = document.createElement('span');
        icon.className = 'tree-icon';
        icon.textContent = type === 'folder' ? '📁' : '📄';
        
        const label = document.createElement('span');
        label.className = 'tree-label';
        label.textContent = name;
        
        content.appendChild(icon);
        content.appendChild(label);
        item.appendChild(content);
        
        if (type === 'folder') {
            const children = document.createElement('div');
            children.className = 'tree-children';
            item.appendChild(children);
            
            content.addEventListener('click', () => {
                item.classList.toggle('expanded');
                icon.textContent = item.classList.contains('expanded') ? '📂' : '📁';
            });
        }
        
        return item;
    }

    // Load and display file
    async function loadFile(folder, filename) {
        const contentDisplay = document.getElementById('content-display');
        const progressOverlay = document.getElementById('progress-overlay');
        const progressBar = document.getElementById('progress-bar');
        const progressText = document.getElementById('progress-text');

        try {
            // Reset state
            state.currentOffset = 0;
            state.fileContent = '';
            progressBar.style.width = '0%';
            
            // Show progress overlay
            progressOverlay.style.display = 'flex';

            // Check file size
            const headResponse = await fetch(`/api/data/${folder}/${filename}`, {
                method: 'HEAD'
            });
            
            const fileSize = parseInt(headResponse.headers.get('content-length'));
            
            if (fileSize > config.maxSafeSize) {
                const shouldLoad = confirm(
                    `This file is large (${formatSize(fileSize)}) and may take a while to load. ` +
                    `Would you like to proceed?`
                );
                if (!shouldLoad) {
                    progressOverlay.style.display = 'none';
                    return;
                }
            }

            // Fetch file content with progress tracking
            const response = await fetch(`/api/data/${folder}/${filename}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);

            const reader = response.body.getReader();
            let receivedLength = 0;
            const chunks = [];

            while (true) {
                const {done, value} = await reader.read();
                
                if (done) break;
                
                chunks.push(value);
                receivedLength += value.length;
                
                // Update progress
                const progress = (receivedLength / fileSize) * 100;
                progressBar.style.width = `${progress}%`;
                progressText.textContent = 
                    `Downloaded ${formatSize(receivedLength)} of ${formatSize(fileSize)} ` +
                    `(${Math.round(progress)}%)`;
            }

            // Combine chunks and convert to text
            const allChunks = new Uint8Array(receivedLength);
            let position = 0;
            for (const chunk of chunks) {
                allChunks.set(chunk, position);
                position += chunk.length;
            }
            
            state.fileContent = new TextDecoder().decode(allChunks);
            state.currentFile = {folder, filename};

            // Display content with pagination
            displayContent();

        } catch (error) {
            console.error('Error loading file:', error);
            contentDisplay.innerHTML = `
                <div style="color: #dc3545; padding: 20px;">
                    Error loading file: ${error.message}
                </div>`;
        } finally {
            progressOverlay.style.display = 'none';
        }
    }

    // Display content with pagination
    function displayContent() {
        const contentDisplay = document.getElementById('content-display');
        const totalPages = Math.ceil(state.fileContent.length / config.chunkSize);
        
        // Create container
        contentDisplay.innerHTML = '';
        
        // Add pagination if needed
        if (totalPages > 1) {
            const pagination = createPagination(totalPages);
            contentDisplay.appendChild(pagination);
        }
        
        // Add content
        const content = document.createElement('div');
        content.className = 'file-content';
        const chunk = state.fileContent.slice(
            state.currentOffset,
            state.currentOffset + config.chunkSize
        );
        content.textContent = chunk;
        contentDisplay.appendChild(content);
    }

    // Create pagination controls
    function createPagination(totalPages) {
        const currentPage = Math.floor(state.currentOffset / config.chunkSize) + 1;
        
        const container = document.createElement('div');
        container.className = 'pagination';
        
        const prevButton = document.createElement('button');
        prevButton.textContent = 'Previous';
        prevButton.disabled = currentPage === 1;
        prevButton.addEventListener('click', () => {
            if (state.currentOffset >= config.chunkSize) {
                state.currentOffset -= config.chunkSize;
                displayContent();
            }
        });
        
        const nextButton = document.createElement('button');
        nextButton.textContent = 'Next';
        nextButton.disabled = currentPage === totalPages;
        nextButton.addEventListener('click', () => {
            if (state.currentOffset + config.chunkSize < state.fileContent.length) {
                state.currentOffset += config.chunkSize;
                displayContent();
            }
        });
        
        const info = document.createElement('div');
        info.className = 'pagination-info';
        info.textContent = `Page ${currentPage} of ${totalPages}`;
        
        container.appendChild(prevButton);
        container.appendChild(info);
        container.appendChild(nextButton);
        
        return container;
    }

    // Format file size
    function formatSize(bytes) {
        const units = ['B', 'KB', 'MB', 'GB'];
        let size = bytes;
        let unitIndex = 0;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return `${size.toFixed(1)} ${units[unitIndex]}`;
    }

    // Start the application
    init();
})();