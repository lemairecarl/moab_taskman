import subprocess
import inspect
import time
from os import makedirs
from os.path import expandvars

homedir = expandvars('$HOME')


class Taskman(object):
    @staticmethod
    def get_moab_queue():
        args = ['showq', '-w', expandvars('user=$USER'), '--blocking']
        try:
            output = subprocess.check_output(args, stderr=subprocess.STDOUT, timeout=10)
        except subprocess.CalledProcessError as e:
            print('Error with showq')
            print(e.output)
            raise
        except subprocess.TimeoutExpired:
            return None, None, None

        showq_lines = output.decode('UTF-8').split('\n')
        showq_lines = [l.strip() for l in showq_lines]
        lists = {'active j': [], 'eligible': [], 'blocked ': []}
        cur_list = None
        for line in showq_lines:
            if line[:8] in lists:
                cur_list = line[:8]
            elif line != '' and \
                            'JOBID' not in line and \
                            'processors' not in line and \
                            'nodes' not in line and \
                            'eligible' not in line and \
                            'Total' not in line and \
                            'blocked' not in line:
                moab_id = line.split(' ')[0]
                lists[cur_list].append(moab_id)
        return lists['active j'], lists['eligible'], lists['blocked ']

    @staticmethod
    def _submit(template_file, args_str, task_name, continue_id=None):
        # Generate id
        new_script = True
        if continue_id is None:
            task_id = time.strftime("%Y-%m-%d_%H-%M-%S")
        else:
            task_id = continue_id
            new_script = False

        script_path = homedir + '/script_moab/taskman/' + task_name
        script_file = script_path + '/' + task_id + '.sh'

        # Create script if new task
        if new_script:
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
            makedirs(script_path, exist_ok=True)
            with open(script_file, 'w') as f:
                f.writelines(script_lines)

            print('Created', script_file)
        else:
            print('Continue using', script_file)

        # Submit using msub
        output = ""
        try:
            print('Calling msub...')
            output = subprocess.check_output(['msub', script_file], stderr=subprocess.STDOUT, timeout=20)

            # Get moab job id
            moab_id = output.decode('UTF-8').strip()

            # Add to 'started' database
            with open(homedir + '/taskman/started', 'a') as f:
                line = '{};{};{};{};{}'.format(task_id, task_name, moab_id, script_file, args_str)
                f.write(line + '\n')

            print('Submitted!  TaskmanID: {}  MoabID: {}'.format(task_id, moab_id))
        except subprocess.CalledProcessError as e:
            print('ERROR using msub:')
            print(e.output)
        except subprocess.TimeoutExpired as e:
            print('TIMEOUT using msub:')
            print(e.output)

        print('====')

    def submit(self, template_file, args_str, task_name):
        self._submit(template_file, args_str, task_name)

    def continu(self, template_file, args_str, task_name, taskman_id):
        self._submit(template_file, args_str, task_name, continue_id=taskman_id)

    cmds = {'sub': submit, 'cont': continu}

    def handle_command(self, cmd_str):
        tokens = cmd_str.split(' ')
        cmd_name = tokens[0]
        if cmd_name == '':
            return
        cmd_args = ' '.join(tokens[1:])
        self.cmds[cmd_name](*cmd_args.split(';'))

    def show_commands(self):
        print('Available commands:')
        for name, fn in self.cmds.items():
            sig = inspect.signature(fn)
            params = list(sig.parameters.values())
            print(name, ':', '; '.join([str(p) for p in params]))

    def show_status(self):
        active_jobs, eligible_jobs, blocked_jobs = self.get_moab_queue()

        with open(homedir + '/taskman/started', 'r') as f:
            started_tasks_csv = f.readlines()
        with open(homedir + '/taskman/dead', 'r') as f:
            dead_tasks_csv = f.readlines()
        with open(homedir + '/taskman/finished', 'r') as f:
            finished_tasks_csv = f.readlines()

        started_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(';') for l in started_tasks_csv]}
        dead_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in dead_tasks_csv]}
        finished_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in finished_tasks_csv]}

        print('\033[2J\033[H')  # Clear screen and move cursor to top left
        print('\033[97;45m( Moab Task Manager )\033[0m     ' + time.strftime("%H:%M:%S"), end='')
        print('     \033[37mCtrl+C to enter command mode\033[0m')
        print('\033[1m{:<8} {:<30} {:<19} {}\033[0m'.format('Status', 'Task name', 'Task id', 'Moab id'))
        for task_id, fields in sorted(started_tasks.items(), key=lambda x: x[1][0]):
            moab_id = fields[1]
            if moab_id in dead_tasks:
                status = '\033[31mDead\033[0m'
            elif moab_id in finished_tasks:
                status = 'Finished'
            elif active_jobs is None:
                status = '?'  # showq has timed out
            elif moab_id in active_jobs:
                status = 'Running'
            elif moab_id in eligible_jobs:
                status = 'Waiting'
            elif moab_id in blocked_jobs:
                status = 'Blocked'
            else:
                status = '\033[31mLost\033[0m'

            status_line = '{:<8} {:<30} {:<19} {}'.format(status, fields[0], task_id, moab_id)
            print(status_line)


if __name__ == '__main__':
    taskman = Taskman()
    while True:
        command_mode = False
        try:
            taskman.show_status()
            time.sleep(4)
        except KeyboardInterrupt:
            command_mode = True

        if command_mode:
            print()
            taskman.show_commands()
            command = input('\033[1mCommand>>\033[0m ')
            taskman.handle_command(command)
