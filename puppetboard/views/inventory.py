from flask import (
    render_template, request, render_template_string
)
from pypuppetdb.QueryBuilder import (AndOperator,
                                     EqualsOperator, OrOperator)

from puppetboard.core import get_app, get_puppetdb, environments
from puppetboard.utils import (check_env)

app = get_app()
puppetdb = get_puppetdb()


def inventory_facts():
    # a list of facts descriptions to go in table header
    headers = []
    # a list of inventory fact names
    fact_names = []

    # load the list of items/facts we want in our inventory
    inv_facts = app.config['INVENTORY_FACTS']

    # generate a list of descriptions and a list of fact names
    # from the list of tuples inv_facts.
    for desc, name in inv_facts:
        headers.append(desc)
        fact_names.append(name)

    return headers, fact_names


@app.route('/inventory', defaults={'env': app.config['DEFAULT_ENVIRONMENT']})
@app.route('/<env>/inventory')
def inventory(env):
    """Fetch all (active) nodes from PuppetDB and stream a table displaying
    those nodes along with a set of facts about them.

    :param env: Search for facts in this environment
    :type env: :obj:`string`
    """
    envs = environments()
    check_env(env, envs)
    headers, fact_names = inventory_facts()

    return render_template(
        'inventory.html',
        envs=envs,
        current_env=env,
        fact_headers=headers,
        is_server_side=app.config.get('SERVER_SIDE_QUERIES'))


@app.route('/inventory/json', defaults={'env': app.config['DEFAULT_ENVIRONMENT']})
@app.route('/<env>/inventory/json')
def inventory_ajax(env):
    """Backend endpoint for inventory table"""
    is_server_side = app.config.get('SERVER_SIDE_QUERIES')
    draw = int(request.args.get('draw', 0))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', app.config['NORMAL_TABLE_COUNT']))
    paging_args = {'limit': length, 'offset': start}

    envs = environments()
    check_env(env, envs)
    headers, fact_names = inventory_facts()
    fact_templates = app.config['INVENTORY_FACT_TEMPLATES']
    fact_data = {}

    if is_server_side:

        if env != '*':
            query = AndOperator()
            query.add(EqualsOperator("environment", env))
        else:
            query = None

        nodes = puppetdb.inventory(
            query=query,
            include_total=True,
            **paging_args
        )

        for node in nodes:
            fact_data[node.node] = {}
            for fact_name in fact_names:
                fact_value = node.facts.get(fact_name, '')
                if fact_name in fact_templates:
                    fact_template = fact_templates[fact_name]
                    fact_value = render_template_string(
                        fact_template,
                        current_env=env,
                        value=fact_value,
                    )
                fact_data[node.node][fact_name] = fact_value

        total = puppetdb.total
    else:
        query = AndOperator()
        fact_query = OrOperator()
        fact_query.add([EqualsOperator("name", name) for name in fact_names])
        query.add(fact_query)

        if env != '*':
            query.add(EqualsOperator("environment", env))

        facts = puppetdb.facts(query=query)

        for fact in facts:
            if fact.node not in fact_data:
                fact_data[fact.node] = {}

            fact_value = fact.value

            if fact.name in fact_templates:
                fact_template = fact_templates[fact.name]
                fact_value = render_template_string(
                    fact_template,
                    current_env=env,
                    value=fact_value,
                )

            fact_data[fact.node][fact.name] = fact_value

        total = len(fact_data)

    return render_template(
        'inventory.json.tpl',
        draw=draw,
        total=total,
        total_filtered=total,
        fact_data=fact_data,
        columns=fact_names)
