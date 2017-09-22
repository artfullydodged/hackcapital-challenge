const { Readable } = require('readable-stream');
const fs = require('fs');

const streamEvents = Readable();
const wstream = fs.createWriteStream('streaming_git.log');

const teams = ['A', 'B', 'C', 'D', 'E'];
const repos = {
  'A': ['proj1'],
  'B': ['proj1', 'proj2', 'proj3'],
  'C': ['proj1', 'proj2', 'proj3', 'proj4'],
  'D': ['proj1', 'proj2'],
  'E': ['proj1', 'proj2', 'proj3']
};
const events = ['push', 'push', 'push', 'issue', 'issue',
  'issue', 'issue-comment', 'issue-comment', 'pull-request', 'pull-request'];

const eventBase = {
  repo: {
    name: '',
    href: '',
  },
  org: {
    login: '',
    href: '',
    avatar: ''
  },
  type: '',
  created_at: '',
};

streamEvents._read = () => {};
const growLog = setInterval(() => {
  let team = teams[Math.floor(Math.random() * teams.length)];
  let repo = repos[team][Math.floor(Math.random() * repos[team].length)];
  let event = events[Math.floor(Math.random() * events.length)];
  let curEvent = JSON.parse(JSON.stringify(eventBase).replace('\n', ''));
  // let curEvent = {};
  curEvent.org.login = team;
  curEvent.org.href = `http://git-test/${team}/`;
  curEvent.org.avatar = `http://git-test/${team}/pic`;
  curEvent.repo.name = `${team}/${repo}`;
  curEvent.repo.href = `http://git-test/${team}/${repo}`;
  curEvent.type = event;
  curEvent.created_at = new Date();
  curEvent.payload = {
    action: 'created',
    created_at: new Date(),
    org: curEvent.org,
    repo: curEvent.repo,
    user: {
      login: 'test',
      avatar: 'http://git-test/test/pic'
    },
    submitter: {
      login: 'test',
      avatar: 'http://git-test/test/pic'
    },
    author: {
      login: 'test',
      avatar: 'http://git-test/test/pic'
    }
  };
  streamEvents.push(JSON.stringify(curEvent) + '\n');
}, 1000);

console.log('Steaming Events...');
streamEvents.pipe(wstream);