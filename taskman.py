import subprocess
import inspect
import time

from os import makedirs


def submit(template_file, args_str, task_name):
    # Generate id
    task_id = time.strftime("%Y-%m-%d_%H-%M-%S")

    # Get template
    with open(template_file, 'r') as f:
        script_lines = f.readlines()

    # Append post exec bash script
    with open('$HOME/script_moab/taskman_post_exec.sh', 'r') as f:
        post_exec = f.readlines()
    script_lines += post_exec

    # Replace variables
    for line in script_lines:
        line.replace('$TASKMAN_NAME', task_name)
        line.replace('$TASKMAN_ID', task_id)
        line.replace('$TASKMAN_ARGS', args_str)

    # Write script
    script_path = '$HOME/script_moab/taskman/' + task_name
    makedirs(script_path)
    script_file = script_path + '/' + task_id + '.sh'
    with open(script_file, 'w') as f:
        f.writelines(script_lines)

    # Submit using msub
    output = ""
    try:
        output = subprocess.check_output(['msub', script_file], stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError:
        print('ERROR using msub:')
        print(output)

    # Get moab job id
    moab_id = output.strip()


cmds = {'sub': submit}


def handle_command(cmd_str):
    tokens = cmd_str.split(' ')
    cmd_name = tokens[0]
    cmd_args = ' '.join(tokens[1:])
    cmds[cmd_name](cmd_args.split(','))


def show_commands():
    print('Available commands:')
    for name, fn in cmds.items():
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        print(name, ':', ', '.join([str(p) for p in params]))


while True:
    try:
        # Update status
        # Show status
        print('Dummy status', time.time(), end='\r')
        time.sleep(4)
    except KeyboardInterrupt:
        # Command mode
        print()
        show_commands()
        command = input('Command>> ')
        handle_command(command)
