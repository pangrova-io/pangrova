function loadQueries() { return fetch('/api/queries').then(r => r.json()); }
