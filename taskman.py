import subprocess
import inspect
import time
from os import makedirs
from os.path import expandvars


homedir = expandvars('$HOME')


def submit(template_file, args_str, task_name):
    # Generate id
    task_id = time.strftime("%Y-%m-%d_%H-%M-%S")

    # Get template
    with open(homedir + '/script_moab/' + template_file + '.sh', 'r') as f:
        template = f.readlines()

    # Append post exec bash script
    with open(homedir + '/script_moab/taskman_post_exec.sh', 'r') as f:
        post_exec = f.readlines()
    template += post_exec

    # Replace variables
    script_lines = []
    for line in template:
        line = line.replace('$TASKMAN_NAME', task_name)
        line = line.replace('$TASKMAN_ID', task_id)
        line = line.replace('$TASKMAN_ARGS', args_str)
        script_lines.append(line)

    # Write script
    script_path = homedir + '/script_moab/taskman/' + task_name
    makedirs(script_path, exist_ok=True)
    script_file = script_path + '/' + task_id + '.sh'
    with open(script_file, 'w') as f:
        f.writelines(script_lines)

    # Submit using msub
    output = ""
    try:
        print('Calling msub...')
        output = subprocess.check_output(['msub', script_file], stderr=subprocess.STDOUT, timeout=20)

        # Get moab job id
        moab_id = output.decode('UTF-8').strip()

        # Add to 'started' database
        with open(homedir + '/taskman/started', 'a') as f:
            line = '{},{},{},{},{}'.format(task_id, task_name, moab_id, template_file, args_str)
            f.write(line + '\n')

        print('Submitted!  TaskmanID: {}  MoabID: {}'.format(task_id, moab_id))
    except subprocess.CalledProcessError as e:
        print('ERROR using msub:')
        print(e.output)
    except subprocess.TimeoutExpired as e:
        print('TIMEOUT using msub:')
        print(e.output)

    print('====')


cmds = {'sub': submit}


def handle_command(cmd_str):
    tokens = cmd_str.split(' ')
    cmd_name = tokens[0]
    cmd_args = ' '.join(tokens[1:])
    cmds[cmd_name](*cmd_args.split(','))


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
        command = input('\033[1mCommand>>\033[0m ')
        handle_command(command)
