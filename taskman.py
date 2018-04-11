import subprocess
import inspect
import time
from os import makedirs
from os.path import expandvars

homedir = expandvars('$HOME')


class Job(object):
    def __init__(self, task_id, name, moab_id, status, script_file, args_str):
        self.task_id = task_id
        self.moab_id = moab_id
        self.name = name
        self.status = status
        self.script_file = script_file
        self.args_str = args_str
        self.report = None


class Taskman(object):
    jobs = {}

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
    def create_task(template_file, args_str, task_name):
        # Generate id
        task_id = time.strftime("%Y-%m-%d_%H-%M-%S")
        script_path = homedir + '/script_moab/taskman/' + task_name
        script_file = script_path + '/' + task_id + '.sh'

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
        return Job(task_name, None, None, script_file, args_str)

    @staticmethod
    def submit(job):
        # Submit using msub
        try:
            print('Calling msub...')
            output = subprocess.check_output(['msub', job.script_file], stderr=subprocess.STDOUT, timeout=20)

            # Get moab job id
            moab_id = output.decode('UTF-8').strip()

            # Add to 'started' database
            with open(homedir + '/taskman/started', 'a') as f:
                line = '{};{};{};{};{}'.format(job.task_id, job.name, moab_id, job.script_file, job.args_str)
                f.write(line + '\n')

            print('Submitted!  TaskmanID: {}  MoabID: {}'.format(job.task_id, moab_id))
        except subprocess.CalledProcessError as e:
            print('ERROR using msub:')
            print(e.output)
        except subprocess.TimeoutExpired as e:
            print('TIMEOUT using msub:')
            print(e.output)
        print('====')

    @staticmethod
    def handle_command(cmd_str):
        tokens = cmd_str.split(' ')
        cmd_name = tokens[0]
        if cmd_name == '':
            return
        cmd_args = ' '.join(tokens[1:])
        cmds[cmd_name](*cmd_args.split(';'))

    @staticmethod
    def show_commands():
        print('Available commands:')
        for name, fn in cmds.items():
            sig = inspect.signature(fn)
            params = list(sig.parameters.values())
            print(name, ':', '; '.join([str(p) for p in params]))

    @staticmethod
    def update_job_list():
        active_jobs, eligible_jobs, blocked_jobs = Taskman.get_moab_queue()

        with open(homedir + '/taskman/started', 'r') as f:
            started_tasks_csv = f.readlines()
        with open(homedir + '/taskman/dead', 'r') as f:
            dead_tasks_csv = f.readlines()
        with open(homedir + '/taskman/finished', 'r') as f:
            finished_tasks_csv = f.readlines()

        started_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(';') for l in started_tasks_csv]}
        dead_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in dead_tasks_csv]}
        finished_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in finished_tasks_csv]}

        jobs = {}
        for task_id, fields in sorted(started_tasks.items(), key=lambda x: x[1][0]):
            name, moab_id, script_file, args_str = fields
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

            jobs[task_id] = Job(task_id, name, moab_id, status, script_file, args_str)
        Taskman.jobs = jobs

    @staticmethod
    def show_status():
        print('\033[2J\033[H')  # Clear screen and move cursor to top left
        print('\033[97;45m( Moab Task Manager )\033[0m     ' + time.strftime("%H:%M:%S"), end='')
        print('     \033[37mCtrl+C to enter command mode\033[0m')
        print('\033[1m{:<8} {:<30} {:<19} {}\033[0m'.format('Status', 'Task name', 'Task id', 'Moab id'))
        for task_id, job in sorted(Taskman.jobs.items(), key=lambda x: x[1].name):
            status_line = '{:<8} {:<30} {:<19} {}'.format(job.status, job.name, task_id, job.moab_id)
            print(status_line)


def submit(template_file, args_str, task_name):
    job = Taskman.create_task(template_file, args_str, task_name)
    Taskman.submit(job)


def continu(task_name):
    match = lambda x: x.startswith(task_name[:-1]) if task_name.endswith('*') else lambda x: x == task_name
    for task_id, job in Taskman.jobs.items():
        if job.status == 'Finished' and match(job.name):
            Taskman.submit(job)


# Available commands
cmds = {'sub': submit, 'cont': continu}


if __name__ == '__main__':
    taskman = Taskman()
    while True:
        command_mode = False
        try:
            taskman.update_job_list()
            taskman.show_status()
            time.sleep(4)
        except KeyboardInterrupt:
            command_mode = True

        if command_mode:
            print()
            taskman.show_commands()
            command = input('\033[1mCommand>>\033[0m ')
            taskman.handle_command(command)
