-record(test, { seed,
                node_count,
                nodes,
                config,
                valid_config,
                active_config
              }).

-record(node, { name,
                port,
                state,
                pid
              }).

-record(config, { version,
                  nodes,
                  gospel,
                  shutdown_timeout }).

-record(step, { modify_node_instrs,
                modify_config_instr,
                existential_node_instr,
                final_state }).
