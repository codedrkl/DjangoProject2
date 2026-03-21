import os


def bundle_project():
    # Configuration
    project_root = os.getcwd()
    output_file = "project_master_bundle.txt"

    # Exclusion list to maintain high-density relevant code
    exclude_dirs = {
        '.venv', 'venv', '__pycache__', '.git', 'migrations',
        'staticfiles', 'media', '.idea', '.vscode'
    }

    py_files = []
    html_files = []

    print(f"🔍 Analyzing project architecture at: {project_root}")

    # Step 1: Sequential Scan
    for root, dirs, files in os.walk(project_root):
        # Filter out noise directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, project_root)

            if file.endswith('.py') and file != 'bundle_project.py':
                py_files.append((relative_path, file_path))
            elif file.endswith('.html'):
                html_files.append((relative_path, file_path))

    # Step 2: Consolidated Output Generation
    with open(output_file, 'w', encoding='utf-8') as master:
        # Header
        master.write(f"{'#' * 100}\n")
        master.write(f"PROJECT MASTER BUNDLE - {os.path.basename(project_root).upper()}\n")
        master.write(f"{'#' * 100}\n\n")

        # Python Section
        master.write(f"{'=' * 30} SECTION 1: PYTHON BACKEND (.py) {'=' * 30}\n\n")
        for rel_path, full_path in sorted(py_files):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    master.write(f"[FILE_START: {rel_path}]\n")
                    master.write(f"{'-' * 40}\n")
                    master.write(content)
                    master.write(f"\n[FILE_END: {rel_path}]\n\n")
            except Exception as e:
                print(f"⚠️ Failed to read {rel_path}: {e}")

        # HTML Section
        master.write(f"\n{'=' * 30} SECTION 2: DJANGO TEMPLATES (.html) {'=' * 30}\n\n")
        for rel_path, full_path in sorted(html_files):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    master.write(f"[FILE_START: {rel_path}]\n")
                    master.write(f"{'-' * 40}\n")
                    master.write(content)
                    master.write(f"\n[FILE_END: {rel_path}]\n\n")
            except Exception as e:
                print(f"⚠️ Failed to read {rel_path}: {e}")

    print(f"✅ Master bundle created: {output_file}")
    print(f"📊 Summary: {len(py_files)} Python files, {len(html_files)} HTML files processed.")


if __name__ == "__main__":
    bundle_project()