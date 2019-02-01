import json
import subprocess
import inspect
import time
import shutil
import math
from datetime import datetime
from enum import Enum
from os import makedirs, environ as env_vars
from os.path import expandvars

HOMEDIR = expandvars('$HOME')
DB_STARTED_TASKS = HOMEDIR + '/taskman/started'
SCRIPTS_FOLDER = env_vars.get('TASKMAN_SCRIPTS', HOMEDIR + '/script_moab')  # Dir with your scripts. Contains /taskman
CKPT_FOLDER = env_vars['TASKMAN_CKPTS']
SLURM_MODE = 'TASKMAN_USE_SLURM' in env_vars
MAX_LINES = env_vars.get('TASKMAN_MAXLINES', 30)


def fmt_time(seconds):
    if seconds >= 3600:
        return str(round(seconds / 3600)) + 'h'
    elif seconds >= 60:
        return str(round(seconds / 60)) + 'm'
    else:
        return str(round(seconds)) + 's'


class JobStatus(Enum):
    Dead = 'Dead'
    Finished = 'Finished'
    Unknown = '?'
    Running = 'Running'
    Waiting = 'Waiting'
    Lost = 'Lost'
    Other = ''

    def __str__(self):
        return self.value

    @property
    def cancellable(self):
        return self in [JobStatus.Running, JobStatus.Waiting]

    @property
    def needs_attention(self):
        return self in [JobStatus.Dead, JobStatus.Lost]


class Job(object):
    def __init__(self, task_id, name, moab_id, status, template_file, args_str):
        self.task_id = task_id
        self.moab_id = moab_id
        self.name = name
        self.status = status
        self.status_msg = None
        self.template_file = template_file
        self.args_str = args_str
        self.report = {}
        self.finish_msg = ''
        self.prev_moab_id = ''

    @property
    def script_file(self):
        _, script_file = Job.get_path(self.name, self.task_id)
        return script_file

    @staticmethod
    def get_path(task_name, task_id):
        script_path = SCRIPTS_FOLDER + '/taskman/' + task_name
        script_file = script_path + '/' + task_id + '.sh'
        return script_path, script_file


class Taskman(object):
    jobs = {}
    columns = set()

    @staticmethod
    def get_cmd_output(args, timeout=20):
        try:
            output = subprocess.check_output(args, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.CalledProcessError as e:
            print('Error with command: ' + ' '.join(args))
            print(e.output)
            raise
        except subprocess.TimeoutExpired as e:
            print('Timeout with command: ' + ' '.join(args))
            print(e.output)
            return None
        return output.decode('UTF-8')

    @staticmethod
    def get_queue():
        if SLURM_MODE:
            return Taskman.get_slurm_queue()
        else:
            return Taskman.get_moab_queue()

    @staticmethod
    def get_moab_queue():
        args = ['showq', '-w', expandvars('user=$USER'), '--blocking']
        output = Taskman.get_cmd_output(args, timeout=10)
        if output is None:
            return None

        showq_lines = output.split('\n')
        showq_lines = [l.strip() for l in showq_lines]
        lists = {'active j': [], 'eligible': [], 'blocked ': []}
        cur_list = None
        statuses = {}
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
                statuses[moab_id] = cur_list
        return statuses

    @staticmethod
    def get_slurm_queue():
        args = ['squeue', '-u', expandvars('$USER')]
        output = Taskman.get_cmd_output(args, timeout=10)
        if output is None:
            return None

        showq_lines = output.split('\n')
        showq_lines = [l for l in showq_lines]
        statuses = {}
        for line in showq_lines[1:]:  # skip header
            slurm_id = line[:8].strip()
            slurm_state = line[47:50].strip()
            statuses[slurm_id] = slurm_state
        return statuses

    @staticmethod
    def generate_script(job):
        script_path, script_file = Job.get_path(job.name, job.task_id)

        # Get template
        with open(SCRIPTS_FOLDER + '/' + job.template_file + '.sh', 'r') as f:
            template = f.readlines()

        # Append post exec bash script
        with open(SCRIPTS_FOLDER + '/taskman_post_exec.sh', 'r') as f:
            post_exec = f.readlines()
        template += post_exec

        # Replace variables
        script_lines = []
        for line in template:
            line = line.replace('$TASKMAN_NAME', job.name)
            line = line.replace('$TASKMAN_ID', job.task_id)
            line = line.replace('$TASKMAN_ARGS', job.args_str)
            script_lines.append(line)

        # Write script
        makedirs(script_path, exist_ok=True)
        with open(script_file, 'w') as f:
            f.writelines(script_lines)

        return script_file

    @staticmethod
    def create_task(template_file, args_str, task_name):
        # Generate id
        task_id = datetime.now().strftime("%m-%d_%H-%M-%S_%f")
        job = Job(task_id, task_name, None, None, template_file, args_str)
        script_file = Taskman.generate_script(job)

        print('Created', script_file)
        return job

    @staticmethod
    def write_started(job, db_file=None):
        if db_file is None:
            f = open(DB_STARTED_TASKS, 'a')
        else:
            f = db_file

        line = '{};{};{};{};{}'.format(job.task_id, job.name, job.moab_id, job.template_file, job.args_str)
        f.write(line + '\n')

        if db_file is None:
            f.close()

    @staticmethod
    def submit(job):
        subm_command = 'sbatch' if SLURM_MODE else 'msub'

        print('Calling ' + subm_command + '...', end=' ')
        output = Taskman.get_cmd_output([subm_command, job.script_file])
        if output is None:
            return

        job.prev_moab_id = job.moab_id or ''
        job.moab_id = output.strip().split(' ')[-1]

        # Add to 'started' database
        Taskman.write_started(job)

        print('Submitted.  TaskmanID: {}  Moab/SLURM ID: {}'.format(job.task_id, job.moab_id))

    @staticmethod
    def cancel(task_id):
        job = Taskman.jobs[task_id]
        cmd_tokens = ['scancel', job.moab_id] if SLURM_MODE else ['mjobctl', '-c', job.moab_id]

        output = Taskman.get_cmd_output(cmd_tokens)
        if output is None:
            return

        # Add to 'finished' database
        with open(HOMEDIR + '/taskman/finished', 'a') as f:
            line = '{},{},{}'.format(job.moab_id, job.name, 'cancel')
            f.write(line + '\n')

        print(output.strip())

    @staticmethod
    def read_task_db():
        with open(HOMEDIR + '/taskman/started', 'r') as f:
            started_tasks_csv = f.readlines()
        with open(HOMEDIR + '/taskman/dead', 'r') as f:
            dead_tasks_csv = f.readlines()
        with open(HOMEDIR + '/taskman/finished', 'r') as f:
            finished_tasks_csv = f.readlines()

        if len(started_tasks_csv) == 0 or started_tasks_csv[0].strip() == '':
            started_tasks = None
        else:
            started_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(';')
                                                                  for l in started_tasks_csv if l.strip() != '']}
        dead_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in dead_tasks_csv]}
        finished_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in finished_tasks_csv]}
        return started_tasks, dead_tasks, finished_tasks

    @staticmethod
    def update_job_list():
        statuses = Taskman.get_queue()

        started_tasks, dead_tasks, finished_tasks = Taskman.read_task_db()
        if started_tasks is None:
            return

        jobs = {}

        for task_id, fields in sorted(started_tasks.items(), key=lambda x: x[1][0]):
            name, moab_id, template_file, args_str = fields
            j = Job(task_id, name, moab_id, None, template_file, args_str)

            if moab_id in dead_tasks:
                j.status = JobStatus.Dead
            elif moab_id in finished_tasks:
                j.status = JobStatus.Finished
                j.finish_msg = finished_tasks[moab_id][1]
            else:
                if statuses is None:
                    j.status = JobStatus.Unknown  # showq has timed out
                elif moab_id not in statuses:
                    j.status = JobStatus.Lost
                elif statuses[moab_id] in ['R', 'active j']:
                    j.status = JobStatus.Running
                elif statuses[moab_id] in ['PD', 'eligible']:
                    j.status = JobStatus.Waiting
                else:
                    j.status = JobStatus.Other
                    j.status_msg = statuses[moab_id]

            jobs[task_id] = j
        Taskman.jobs = jobs
        Taskman.update_report()

    @staticmethod
    def get_log(job, error_log=False):
        ext_prefix = '.e' if error_log else '.o'
        moab_id = job.prev_moab_id if job.status in [JobStatus.Waiting,
                                                     JobStatus.Unknown, JobStatus.Other] else job.moab_id
        output_filepath = HOMEDIR + '/logs/' + job.name + ext_prefix + moab_id
        try:
            with open(output_filepath, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = None
        return lines, output_filepath

    @staticmethod
    def update_report():
        Taskman.columns = set()
        for task_id, job in Taskman.jobs.items():
            log_lines, _ = Taskman.get_log(job)
            if log_lines is not None:
                report_line = None
                for line in log_lines:
                    if line[:8] == '!taskman':
                        report_line = line
                if report_line is not None:
                    job.report = json.loads(report_line[8:])
                    Taskman.columns.update(job.report.keys())
        if 'time' in Taskman.columns:
            Taskman.columns.remove('time')

    @staticmethod
    def resume_incomplete_tasks():
        for task_id, job in Taskman.jobs.items():
            if job.status != JobStatus.Finished:
                continue
            do_resubmit = job.report.get('resubmit', False)
            if do_resubmit:
                Taskman.submit(job)
        time.sleep(2)

    @staticmethod
    def show_status():
        print('\033[2J\033[H')  # Clear screen and move cursor to top left
        print('\033[97;45m( Experiment Manager )\033[0m     ' + time.strftime("%H:%M:%S"), end='')
        print('     \033[37mCtrl+C to enter command mode\033[0m')

        line_fmt = '{:<8} {:<30} {:<21} {:<7} {:<7}' + ' {:<12}' * len(Taskman.columns)
        print('\033[1m' + line_fmt.format('Status', 'Task name', 'Task id', 'Moab id', 'Updated',
                                          *sorted(Taskman.columns)) + '\033[0m')
        
        waiting_tasks = [j for j in Taskman.jobs if j.status == JobStatus.Waiting]
        non_waiting_tasks = [j for j in Taskman.jobs if j.status != JobStatus.Waiting]
        
        def print_job_line(job):
            # Get report data
            report_columns = []
            for k in sorted(Taskman.columns):
                val_str = str(job.report.get(k, ''))[:12]
                report_columns.append(val_str)
            time_ago = fmt_time(time.time() - job.report['time']) if 'time' in job.report else ''
            # Format line
            status_line = line_fmt.format(job.status, short_str(job.name, 30), job.task_id, job.moab_id, time_ago,
                                          *report_columns)
            if job.status.needs_attention:
                status_line = '\033[31m' + status_line + '\033[0m'
            elif job.status == JobStatus.Other:
                status_line = '\033[30;47m' + job.status_msg[:8].ljust(8) + status_line[8:] + '\033[0m'
            elif job.status == JobStatus.Finished:
                finished_status = {'ok': '\033[32;107mFinished\033[;107m',  # Green
                                   'cancel': '\033[;107mCancel\'d'  # Black
                                   }.get(job.finish_msg, '\033[;107mFinished')
                status_line = finished_status + status_line[8:] + '\033[0m'
            print(status_line)

        for job in list(sorted(non_waiting_tasks, key=lambda x: x.name))[:MAX_LINES]:
            print_job_line(job)
        if len(non_waiting_tasks) < MAX_LINES:
            for job in list(sorted(waiting_tasks, key=lambda x: x.name))[:MAX_LINES - len(non_waiting_tasks)]:
                print_job_line(job)
        if len(Taskman.jobs) > MAX_LINES:
            total_not_shown = MAX_LINES - len(Taskman.jobs)
            print('[ ... {} tasks not shown - {} waiting tasks in total ... ]'.format(total_not_shown,
                                                                                      len(waiting_tasks)))
            

    @staticmethod
    def update(resume_incomplete_tasks=True):
        Taskman.update_job_list()
        Taskman.show_status()
        if resume_incomplete_tasks:
            Taskman.resume_incomplete_tasks()


def _handle_command(cmd_str):
    tokens = cmd_str.split(' ')
    cmd_name = tokens[0]
    if cmd_name == '':
        return
    if len(tokens) == 1:
        cmds[cmd_name]()
    else:
        cmd_args = ' '.join(tokens[1:])
        cmds[cmd_name](*cmd_args.split(';'))


def _show_commands():
    print('-------------------')
    print('Available commands:')
    for name, fn in sorted(cmds.items(), key=lambda x: x[0]):
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        print(name, ':', '; '.join([str(p) for p in params]))


def _match(pattern, name):
    if pattern.endswith('*'):
        return name.startswith(pattern[:-1])
    else:
        return name == pattern


def submit(template_file, args_str, task_name):
    job = Taskman.create_task(template_file, args_str, task_name)
    Taskman.submit(job)


def fromckpt(template_file, args_str, task_name, ckpt_file):
    job = Taskman.create_task(template_file, args_str, task_name)
    print('Moving checkpoint...')
    job_dir = CKPT_FOLDER + '/' + job.name + '/' + job.task_id
    makedirs(job_dir)
    shutil.move(HOMEDIR + '/' + ckpt_file, job_dir)
    Taskman.submit(job)


def multi_sub():
    print('Enter multiple submission lines. Add an empty line to end.')
    print()
    a = []
    while True:
        i = input()
        if i == '':
            break
        a.append(i)
    print('Tasks to submit:')
    for i in a:
        print(i)
    print()
    r = input('Submit? (y/n)')
    if r == 'y':
        for i in a:
            submit(*i.split(';'))


def continu(task_name):
    for task_id, job in Taskman.jobs.items():
        if (job.status in [JobStatus.Dead, JobStatus.Lost] or job.status == JobStatus.Finished
                and job.finish_msg == 'cancel') and _match(task_name, job.name):
            Taskman.submit(job)


def cancel(task_name):
    for task_id, job in Taskman.jobs.items():
        if job.status.cancellable and _match(task_name, job.name):
            Taskman.cancel(task_id)


def copy(task_name):
    submitted = set()
    for task_id, job in Taskman.jobs.items():
        if job.name not in submitted and _match(task_name, job.name):
            job = Taskman.create_task(job.template_file, job.args_str, job.name)
            Taskman.submit(job)
            submitted.add(job.name)


def show(task_name):
    print()
    for task_id, job in Taskman.jobs.items():
        if _match(task_name, job.name):
            out_log, out_log_file = Taskman.get_log(job)
            err_log, err_log_file = Taskman.get_log(job, error_log=True)

            print('\033[1m' + job.name + '\033[0m :', job.args_str)
            print('\033[30;44m' + ' ' * 40 + '\033[0m ' + out_log_file + '\r\033[2C Output ')
            if out_log is not None:
                for l in out_log[-20:]:
                    print(l.strip())
            print('\033[30;44m' + ' ' * 40 + '\033[0m ' + err_log_file + '\r\033[2C Error ')
            if err_log is not None:
                for l in err_log[-30:]:
                    print(l.strip())
            print('\033[30;44m' + ' ' * 40 + '\033[0m')
            print()
    input('Press any key...')


def pack(task_name):
    checkpoint_paths = []
    for task_id, job in Taskman.jobs.items():
        if job.status == JobStatus.Finished and _match(task_name, job.name):
            checkpoint_paths.append(job.name + '/' + job.task_id)
    # Call pack.sh
    subprocess.Popen([HOMEDIR + '/taskman/pack.sh'] + checkpoint_paths)


def results(task_name):
    files = []
    for task_id, job in Taskman.jobs.items():
        if job.status == JobStatus.Finished and _match(task_name, job.name):
            filepath = job.name + '/' + job.task_id + '/results.csv'
            files.append(filepath)
    # Call pack.sh
    subprocess.Popen([HOMEDIR + '/taskman/packresults.sh'] + files)


def _clean(task_name=None, clean_all=False):
    shutil.copyfile(DB_STARTED_TASKS,
                    HOMEDIR + '/taskman/old/started_' + datetime.now().strftime("%m-%d_%H-%M-%S"))

    started_tasks, dead_tasks, finished_tasks = Taskman.read_task_db()

    with open(DB_STARTED_TASKS, 'w') as f:
        for task_id, fields in started_tasks.items():
            name, moab_id, template_file, args_str = fields
            remove = clean_all or (moab_id in dead_tasks or moab_id in finished_tasks)
            if task_name is not None:
                remove = _match(task_name, name) and remove
            if not remove:
                job = Job(task_id, name, moab_id, None, template_file, args_str)
                Taskman.write_started(job, f)


def clean(task_name=None):
    _clean(task_name)


def cleanall(task_name=None):
    _clean(task_name, clean_all=True)


def regen_script(task_name):
    for task_id, job in Taskman.jobs.items():
        if _match(task_name, job.name):
            script = Taskman.generate_script(job)
            print('Regenerated', script)


def short_str(x, l):
    """Shorten string from the center"""
    left_side = math.floor((l - 2) / 2)
    right_side = (l - 2) - left_side
    return x[:left_side] + '..' + x[-right_side:]


# Available commands
cmds = {'sub': submit, 'fromckpt': fromckpt, 'multisub': multi_sub, 'cont': continu, 'cancel': cancel, 'copy': copy,
        'pack': pack, 'results': results, 'show': show, 'clean': clean, 'cleanall': cleanall, 'regen': regen_script}


if __name__ == '__main__':
    while True:
        command_mode = False
        try:
            Taskman.update()
            time.sleep(120)
        except KeyboardInterrupt:
            command_mode = True

        if command_mode:
            print('\rUpdating, please wait...')
            Taskman.update(resume_incomplete_tasks=False)
            _show_commands()
            command = input('\033[1mCommand>>\033[0m ')
            _handle_command(command)
