import subprocess
import inspect
import time


def submit(template_file, args_str):
    print(template_file)
    print(args_str)


cmds = {'sub': submit}


def handle_command(cmd_str):
    tokens = cmd_str.split(' ')
    cmds[tokens[0]](*tokens[1:])


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
