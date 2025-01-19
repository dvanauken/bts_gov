import glob
import os
from datetime import datetime
from pathlib import Path
from typing import List, Set
from tqdm import tqdm

class CodeString:
    class Paths:
        filename = None  # Class variable to track the output file

        def __init__(self, base_dir: str = ".", includeFileContent: bool = False, debug: bool = False):
            self.base_dir = os.path.abspath(base_dir)
            self.debug = debug
            self.includeFileContent = includeFileContent
            self.included_paths: Set[str] = set()
            self.excluded_paths: Set[str] = set()
            self.final_paths: List[str] = []
            self.comments: List[str] = []  # Add this line
            
            if self.debug:
                print(f"\nDEBUG: Initialized with base_dir: {self.base_dir}")
                print(f"DEBUG: Base dir exists: {os.path.exists(self.base_dir)}")
                print(f"DEBUG: Base dir is directory: {os.path.isdir(self.base_dir)}")
                print(f"DEBUG: Include file content: {self.includeFileContent}")

        def comment(self, text: str) -> 'CodeString.Paths':
            """Add a comment that will appear before the tree output"""
            self.comments.append(text)
            return self

        def include(self, pattern: str) -> 'CodeString.Paths':
            if self.debug:
                print(f"\nDEBUG: Starting include with pattern: {pattern}")
            
            search_pattern = os.path.join(self.base_dir, pattern)
            search_pattern = search_pattern.replace('\\', '/')
            
            if self.debug:
                print(f"DEBUG: Search pattern: {search_pattern}")
            
            matches = glob.glob(search_pattern, recursive=True)
            
            if self.debug:
                print(f"DEBUG: Found {len(matches)} matches")
                if matches:
                    print("DEBUG: First few matches:")
                    for m in matches[:3]:
                        print(f"  {m}")
                else:
                    print("\nDEBUG: No matches found. Checking base directory content:")
                    try:
                        base_content = os.listdir(self.base_dir)
                        print(f"Base dir contains {len(base_content)} items:")
                        for item in base_content[:5]:
                            print(f"  {item}")
                        if len(base_content) > 5:
                            print("  ...")
                    except Exception as e:
                        print(f"DEBUG: Error listing directory: {e}")
            
            with tqdm(matches) as pbar:
                for path in pbar:
                    rel_path = os.path.relpath(path, self.base_dir)
                    if self.debug:
                        print(f"DEBUG: Adding path: {rel_path}")
                    self.included_paths.add(rel_path)
            return self

        def exclude(self, pattern: str) -> 'CodeString.Paths':
            search_pattern = os.path.join(self.base_dir, "**", pattern)
            search_pattern = search_pattern.replace('\\', '/')
            matches = glob.glob(search_pattern, recursive=True)
            
            if self.debug:
                print(f"\nDEBUG: Exclude pattern: {search_pattern}")
                print(f"DEBUG: Found {len(matches)} matches to exclude")
            
            with tqdm(matches) as pbar:
                for path in pbar:
                    rel_path = os.path.relpath(path, self.base_dir)
                    self.excluded_paths.add(rel_path)
            return self
        

        def _clean_file_contents(self, file_path: str) -> str:
            try:
                # Build the full path including all parent directories
                if os.path.isabs(file_path):
                    full_path = file_path
                else:
                    path_parts = file_path.split(os.sep)
                    full_path = os.path.join(self.base_dir, *path_parts)
                    
                if self.debug:
                    print(f"DEBUG: Attempting to read file: {full_path}")
                    
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Process content line by line for replacements
                    lines = content.splitlines()
                    processed_lines = [self.replace(line) for line in lines]
                    # Join and remove excessive whitespace
                    content = ' '.join(' '.join(processed_lines).split())
                    return content
            except Exception as e:
                if self.debug:
                    print(f"DEBUG: Error reading file {file_path}: {str(e)}")
                    print(f"DEBUG: Attempted full path: {full_path}")
                return f"<error reading file: {str(e)}>"

        #def _clean_file_contents(self, file_path: str) -> str:
        #    try:
        #        # Build the full path including all parent directories
        #        if os.path.isabs(file_path):
        #            full_path = file_path
        #        else:
        #            # Join all parts of the path
        #            path_parts = file_path.split(os.sep)
        #            full_path = os.path.join(self.base_dir, *path_parts)
        #            
        #        if self.debug:
        #            print(f"DEBUG: Attempting to read file: {full_path}")
        #            
        #        with open(full_path, 'r', encoding='utf-8') as f:
        #            content = f.read()
        #            # Remove newlines and excessive whitespace
        #           content = ' '.join(content.split())
        #            return content
        #    except Exception as e:
        #        if self.debug:
        #            print(f"DEBUG: Error reading file {file_path}: {str(e)}")
        #            print(f"DEBUG: Attempted full path: {full_path}")
        #        return f"<error reading file: {str(e)}>"

        def _build_tree(self, paths: List[str]) -> dict:
            tree = {}
            for path in paths:
                current = tree
                parts = Path(path).parts
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = {}
            return tree

        def _print_tree(self, tree: dict, prefix: str = "", is_last: bool = True, current_path: str = "") -> List[str]:
            lines = []
            items = list(tree.items())
            
            for i, (name, subtree) in enumerate(items):
                is_last_item = i == len(items) - 1
                connector = "└── " if is_last_item else "├── "
                
                # Build the full relative path
                new_path = os.path.join(current_path, name) if current_path else name
                
                # If it's a leaf node (file)
                if not subtree:  # This means it's a file
                    if self.includeFileContent:
                        content = self._clean_file_contents(new_path)  # Pass the full relative path
                        lines.append(f"{prefix}{connector}{name}: {content}")
                    else:
                        lines.append(f"{prefix}{connector}{name}")
                else:
                    lines.append(f"{prefix}{connector}{name}")
                
                if subtree:
                    extension = "    " if is_last_item else "│   "
                    subtree_lines = self._print_tree(
                        subtree, 
                        prefix + extension,
                        is_last_item,
                        new_path  # Pass the accumulated path
                    )
                    lines.extend(subtree_lines)
            return lines

        def generate(self) -> None:
            if self.debug:
                print("\nDEBUG: Starting generate")
                print(f"DEBUG: Included paths: {len(self.included_paths)}")
                print(f"DEBUG: Excluded paths: {len(self.excluded_paths)}")
            
            if len(self.excluded_paths) > len(self.included_paths):
                all_paths = {os.path.relpath(p, self.base_dir) 
                           for p in glob.glob(os.path.join(self.base_dir, "**"), recursive=True)}
                remaining = all_paths - self.excluded_paths
                self.final_paths = list(remaining & self.included_paths)
            else:
                self.final_paths = list(self.included_paths - self.excluded_paths)
            
            if self.debug:
                print(f"DEBUG: Final paths count: {len(self.final_paths)}")
                if self.final_paths:
                    print("DEBUG: Sample final paths:")
                    for p in self.final_paths[:3]:
                        print(f"  {p}")
            
            self.final_paths.sort()
            tree = self._build_tree(self.final_paths)
            tree_lines = self._print_tree(tree)

            # Combine comments with tree lines
            output_lines = []
            if self.comments:
                output_lines.extend(self.comments)
                output_lines.append("")  # Add blank line after comments
            output_lines.extend(tree_lines)

            # Create filename if it doesn't exist yet
            if not CodeString.Paths.filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                CodeString.Paths.filename = f"result.{timestamp}.txt"

            output_path = os.path.join(os.getcwd(), CodeString.Paths.filename)
            
            if self.debug:
                print(f"\nDEBUG: Writing to: {output_path}")
            
            # Append mode if file exists, write mode if it's the first write
            mode = 'a' if os.path.exists(output_path) else 'w'
            with open(output_path, mode, encoding='utf-8') as f:
                if mode == 'a':  # If appending, add a separator
                    f.write("\n" + "-" * 70 + "\n")
                f.write("\n".join(output_lines) + "\n")
            
            if mode == 'w':  # Only print the message on first write
                print(f"Tree structure written to {output_path}")


        def replace(self, line: str) -> str:
            """Simple string replacement for sanitizing content.
            Add cases here as needed for quick string manipulations."""
            
            # Simple examples of common string operations
            if line.startswith("com.acme."):
                return line.replace("com.acme.", "c.a.")
            
            if line.startswith("import { Component, OnInit }"):
                return line.replace("import { Component, OnInit }", "xxxxxxxxx")

                
            if " com.acme." in line:  # For handling imports/usages in middle of line
                return line.replace(" com.acme.", " c.a.")
                
            if line.endswith(".acme.Test"):  # Example of endsWith check
                return line[:-10] + ".a.Test"
            
            # Most lines will simply pass through unchanged
            return line


    @classmethod
    def explore(cls, base_dir: str = ".", includeFileContent: bool = False, debug: bool = False) -> Paths:
        return cls.Paths(base_dir, includeFileContent, debug)

if __name__ == "__main__":
    # Get root level files
    paths = CodeString.explore(".", includeFileContent=False, debug=True)
    paths.comment("Project: bts_gov")
    paths.comment("===================================================================================================")
    paths.comment("Directory and Folder layout:")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths.include("**/*") \
        .exclude("__pycache__/**") \
        .exclude("venv/**") \
        .exclude("coupon/**") \
        .exclude("market/**") \
        .exclude("venv/**") \
        .exclude("dist/**") \
        .exclude("build/**") \
        .exclude("requirements.txt")\
        .exclude("result.*.txt")\
        .exclude("code_string.py")\
        .exclude("scaffold.ps1")\
        .exclude("tiger_processed/ROADS/*.topojson") \
        .generate()

    # Get configuration files
    paths = CodeString.explore(".", includeFileContent=True, debug=True)
    paths.comment("Config files")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths\
        .exclude("venv/**") \
        .exclude("coupon/**") \
        .exclude("market/**") \
        .include("*.json") \
        .include("*.ini") \
        .include("*.yaml") \
        .include("requirements.txt") \
        .include("setup.py") \
        .include("project.toml") \
        .generate()

    # Get Python source files
    paths = CodeString.explore("./app/server", includeFileContent=True, debug=True)
    paths.comment("Python source files")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths.comment("./app/server:")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths\
        .exclude("venv/**") \
        .exclude("coupon/**") \
        .exclude("code_string.py") \
        .exclude("market/**") \
        .include("**/*.py") \
        .generate()


    # Get Python source files
    paths = CodeString.explore("./app/client", includeFileContent=True, debug=True)
    paths.comment("Client source files")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths.comment("./app/client:")
    paths.comment("---------------------------------------------------------------------------------------------------")
    paths\
        .exclude("venv/**") \
        .exclude("coupon/**") \
        .exclude("market/**") \
        .include("**/*.html") \
        .include("**/*.css") \
        .include("**/*.scss") \
        .include("**/*.js") \
        .generate()
    