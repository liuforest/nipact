BEGIN TRANSACTION;
PRAGMA user_version = 18;
CREATE TABLE artifact_dependencies (
            dependent_artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
            source_artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
            source_content_digest TEXT NOT NULL,
            source_file_size INTEGER NOT NULL CHECK(source_file_size >= 0),
            source_extension TEXT NOT NULL,
            input_path TEXT NOT NULL,
            binding_name TEXT NOT NULL,
            dependency_role TEXT NOT NULL,
            source_step_name TEXT,
            source_output_name TEXT,
            source_address TEXT,
            source_scope TEXT CHECK(
                source_scope IS NULL OR source_scope IN ('global', 'entity')
            ),
            source_name TEXT,
            source_entity_id TEXT,
            source_occurrence_path TEXT,
            dependency_set_id TEXT,
            manifest_value_schema TEXT,
            manifest_digest TEXT,
            edge_cardinality INTEGER CHECK(
                edge_cardinality IS NULL OR edge_cardinality >= 0
            ),
            CHECK (
                (manifest_value_schema IS NULL) = (manifest_digest IS NULL)
            ),
            CHECK (
                (
                    source_scope IS NULL
                    AND source_name IS NULL
                    AND source_entity_id IS NULL
                    AND source_occurrence_path IS NULL
                )
                OR
                (
                    source_scope = 'global'
                    AND source_name IS NOT NULL
                    AND source_entity_id IS NULL
                    AND source_occurrence_path IS NOT NULL
                )
                OR
                (
                    source_scope = 'entity'
                    AND source_name IS NOT NULL
                    AND source_entity_id IS NOT NULL
                    AND source_occurrence_path IS NOT NULL
                )
            ),
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT,
            PRIMARY KEY (
                dependent_artifact_id, source_artifact_id, input_path, binding_name
            )
        );
INSERT INTO "artifact_dependencies" VALUES(2,1,'45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9',21,'.txt','../../../../data/source/entity_001.txt','source_value','source_input',NULL,NULL,NULL,'entity','source_value','entity_001','data/source/entity_001.txt',NULL,NULL,NULL,1);
INSERT INTO "artifact_dependencies" VALUES(3,2,'887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8',63,'.json','staging/fixture_source/source_value/entity_001.json','source_value','source_input','fixture_source','source_value','entity_001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1);
INSERT INTO "artifact_dependencies" VALUES(4,2,'887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8',63,'.json','staging/fixture_source/source_value/entity_001.json','source_value','source_input','fixture_source','source_value','entity_001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1);
INSERT INTO "artifact_dependencies" VALUES(5,3,'8167bac79d7f1a3e14b1778b583adfe20bcfc9f44a202144ac194313d3b74fed',94,'.json','staging/fixture_transform/left/entity_001.json','left_values','analysis_input','fixture_transform','left','entity_001',NULL,NULL,NULL,NULL,NULL,'entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23',1);
INSERT INTO "artifact_dependencies" VALUES(5,4,'ae27d728c8e2eb89f88d0e84f7984de963fca681b9252c67847ee59b8d711c07',95,'.json','staging/fixture_transform/right/entity_001.json','right_values','analysis_input','fixture_transform','right','entity_001',NULL,NULL,NULL,NULL,NULL,'entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23',1);
CREATE TABLE artifacts (
            artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL CHECK(origin IN ('source', 'workflow_output')),
            run_id INTEGER REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT,
            step_name TEXT,
            output_name TEXT,
            address TEXT,
            job_id TEXT,
            artifact_set_id TEXT,
            parameter_id INTEGER REFERENCES parameters(parameter_id),
            path TEXT NOT NULL,
            is_selected_output INTEGER NOT NULL DEFAULT 0
                CHECK(is_selected_output IN (0, 1)),
            is_published INTEGER NOT NULL DEFAULT 0 CHECK(is_published IN (0, 1)),
            published_path TEXT,
            staging_path TEXT,
            content_digest TEXT NOT NULL,
            output_hash TEXT,
            file_size INTEGER NOT NULL CHECK(file_size >= 0),
            extension TEXT NOT NULL,
            subject_id TEXT,
            session_id TEXT,
            task_name TEXT,
            run_label TEXT,
            datatype TEXT,
            suffix TEXT,
            source_metadata_json TEXT,
            callable_ref TEXT,
            software_ref TEXT,
            source_scope TEXT CHECK(
                source_scope IS NULL OR source_scope IN ('global', 'entity')
            ),
            source_name TEXT,
            source_entity_id TEXT,
            source_st_dev INTEGER CHECK(source_st_dev IS NULL OR source_st_dev >= 0),
            source_st_ino INTEGER CHECK(source_st_ino IS NULL OR source_st_ino >= 0),
            source_st_size INTEGER CHECK(source_st_size IS NULL OR source_st_size >= 0),
            source_st_mtime_ns INTEGER CHECK(
                source_st_mtime_ns IS NULL OR source_st_mtime_ns >= 0
            ),
            source_st_ctime_ns INTEGER CHECK(
                source_st_ctime_ns IS NULL OR source_st_ctime_ns >= 0
            ),
            request_bundle_digest TEXT
                REFERENCES request_bundle_projections(request_bundle_digest)
                ON DELETE RESTRICT,
            created_at TEXT NOT NULL,
            CHECK (
                (
                    origin = 'workflow_output'
                    AND request_bundle_digest IS NOT NULL
                )
                OR (
                    origin = 'source'
                    AND request_bundle_digest IS NULL
                )
            ),
            CHECK (
                origin != 'source'
                OR (
                    run_id IS NULL
                    AND workflow_name IS NULL
                    AND step_name IS NULL
                    AND output_name IS NULL
                    AND address IS NULL
                    AND parameter_id IS NULL
                    AND is_selected_output = 0
                    AND is_published = 0
                    AND published_path IS NULL
                    AND staging_path IS NULL
                    AND source_scope IS NOT NULL
                    AND source_name IS NOT NULL
                    AND (
                        (source_scope = 'global' AND source_entity_id IS NULL)
                        OR
                        (source_scope = 'entity' AND source_entity_id IS NOT NULL)
                    )
                    AND source_st_dev IS NOT NULL
                    AND source_st_ino IS NOT NULL
                    AND source_st_size IS NOT NULL
                    AND source_st_mtime_ns IS NOT NULL
                    AND source_st_ctime_ns IS NOT NULL
                )
            ),
            CHECK (
                origin = 'source'
                OR (
                    source_scope IS NULL
                    AND source_name IS NULL
                    AND source_entity_id IS NULL
                    AND source_st_dev IS NULL
                    AND source_st_ino IS NULL
                    AND source_st_size IS NULL
                    AND source_st_mtime_ns IS NULL
                    AND source_st_ctime_ns IS NULL
                )
            )
        );
INSERT INTO "artifacts" VALUES(1,'source',NULL,'v18_fixture',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'data/source/entity_001.txt',0,0,NULL,NULL,'45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9','45e8f93b1f72302e',21,'.txt',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'entity','source_value','entity_001',66307,105397462,21,1786653318121066463,1786655196712098680,NULL,'2026-08-13T21:06:36+00:00');
INSERT INTO "artifacts" VALUES(2,'workflow_output',1,'v18_fixture','base','fixture_source','source_value','entity_001','job__fixture_source__source_value__entity_001',NULL,1,'outputs/v1/v18_fixture/fixture_source/entity_001/2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902/source_value/entity_001.887dedd0914cb8d4.json',0,1,'outputs/v1/v18_fixture/fixture_source/entity_001/2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902/source_value/entity_001.887dedd0914cb8d4.json','runs/v18_fixture/base/fixture_analysis/staging/fixture_source/source_value/entity_001.json','887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8','887dedd0914cb8d4',63,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_source_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902','2026-08-13T21:06:38+00:00');
INSERT INTO "artifacts" VALUES(3,'workflow_output',1,'v18_fixture','base','fixture_transform','left','entity_001','job__fixture_transform__outputs__entity_001',NULL,2,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/left/entity_001.8167bac79d7f1a3e.json',0,1,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/left/entity_001.8167bac79d7f1a3e.json','runs/v18_fixture/base/fixture_analysis/staging/fixture_transform/left/entity_001.json','8167bac79d7f1a3e14b1778b583adfe20bcfc9f44a202144ac194313d3b74fed','8167bac79d7f1a3e',94,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_transform_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2','2026-08-13T21:06:38+00:00');
INSERT INTO "artifacts" VALUES(4,'workflow_output',1,'v18_fixture','base','fixture_transform','right','entity_001','job__fixture_transform__outputs__entity_001',NULL,2,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/right/entity_001.ae27d728c8e2eb89.json',0,1,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/right/entity_001.ae27d728c8e2eb89.json','runs/v18_fixture/base/fixture_analysis/staging/fixture_transform/right/entity_001.json','ae27d728c8e2eb89f88d0e84f7984de963fca681b9252c67847ee59b8d711c07','ae27d728c8e2eb89',95,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_transform_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2','2026-08-13T21:06:38+00:00');
INSERT INTO "artifacts" VALUES(5,'workflow_output',1,'v18_fixture','base','fixture_analysis','summary','cohort','job__fixture_analysis__summary__cohort',NULL,4,'outputs/v1/v18_fixture/fixture_analysis/cohort/b88719ca15a200aebf0a79701c091ecdd229a7036fb03bba6b55683b88aebc85/summary/cohort.2e87cc7d7fcf4488.json',1,1,'outputs/v1/v18_fixture/fixture_analysis/cohort/b88719ca15a200aebf0a79701c091ecdd229a7036fb03bba6b55683b88aebc85/summary/cohort.2e87cc7d7fcf4488.json','runs/v18_fixture/base/fixture_analysis/staging/fixture_analysis/summary/cohort.json','2e87cc7d7fcf4488c1306d0ae93194980226fa63255873f3d2ac7b4082e89a64','2e87cc7d7fcf4488',229,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_analysis_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'b88719ca15a200aebf0a79701c091ecdd229a7036fb03bba6b55683b88aebc85','2026-08-13T21:06:38+00:00');
CREATE TABLE contexts (
            context TEXT PRIMARY KEY,
            runtime_path TEXT NOT NULL,
            storage_layout_version INTEGER NOT NULL DEFAULT 1 CHECK(
                storage_layout_version = 1
            )
        );
INSERT INTO "contexts" VALUES('v18_fixture','/tmp/nipact-v18-runtime.sfKk2f/runtime',1);
CREATE TABLE manifest_declarations (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            manifest_name TEXT NOT NULL,
            declared_path TEXT NOT NULL,
            last_validated_manifest_value_schema TEXT NOT NULL,
            last_validated_manifest_digest TEXT NOT NULL,
            PRIMARY KEY (context, manifest_name),
            FOREIGN KEY (
                last_validated_manifest_value_schema,
                last_validated_manifest_digest
            ) REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
        );
INSERT INTO "manifest_declarations" VALUES('v18_fixture','cohort','manifests/cohort.yaml','entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23');
CREATE TABLE manifest_values (
            value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL CHECK(
                length(manifest_digest) = 64
                AND manifest_digest NOT GLOB '*[^0-9a-f]*'
            ),
            canonical_body TEXT NOT NULL CHECK(length(canonical_body) > 0),
            entity_count INTEGER NOT NULL CHECK(entity_count > 0),
            PRIMARY KEY (value_schema, manifest_digest)
        );
INSERT INTO "manifest_values" VALUES('entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23','entity_001',1);
CREATE TABLE parameters (
            parameter_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_version INTEGER NOT NULL,
            parameter_hash TEXT NOT NULL,
            parameter_digest TEXT NOT NULL,
            step_name TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (hash_version, step_name, parameter_hash)
        );
INSERT INTO "parameters" VALUES(1,1,'44136fa355b3678a','44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a','fixture_source','{}','2026-08-13T21:06:38+00:00');
INSERT INTO "parameters" VALUES(2,1,'5deeee15ee938f3f','5deeee15ee938f3f5f689a10dcf11137d9963ae23f24ec543967fbc6d34c7de3','fixture_transform','{"variant":"base"}','2026-08-13T21:06:38+00:00');
INSERT INTO "parameters" VALUES(4,1,'44136fa355b3678a','44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a','fixture_analysis','{}','2026-08-13T21:06:38+00:00');
CREATE TABLE published_outputs (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT NOT NULL,
            step_name TEXT NOT NULL,
            output_name TEXT NOT NULL,
            address TEXT NOT NULL,
            path TEXT NOT NULL,
            output_digest TEXT NOT NULL,
            output_hash TEXT NOT NULL,
            artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
            PRIMARY KEY (context, workflow_name, step_name, output_name, address)
        );
INSERT INTO "published_outputs" VALUES('v18_fixture','base','fixture_source','source_value','entity_001','outputs/v1/v18_fixture/fixture_source/entity_001/2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902/source_value/entity_001.887dedd0914cb8d4.json','887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8','887dedd0914cb8d4',2);
INSERT INTO "published_outputs" VALUES('v18_fixture','base','fixture_transform','left','entity_001','outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/left/entity_001.8167bac79d7f1a3e.json','8167bac79d7f1a3e14b1778b583adfe20bcfc9f44a202144ac194313d3b74fed','8167bac79d7f1a3e',3);
INSERT INTO "published_outputs" VALUES('v18_fixture','base','fixture_transform','right','entity_001','outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/right/entity_001.ae27d728c8e2eb89.json','ae27d728c8e2eb89f88d0e84f7984de963fca681b9252c67847ee59b8d711c07','ae27d728c8e2eb89',4);
INSERT INTO "published_outputs" VALUES('v18_fixture','base','fixture_analysis','summary','cohort','outputs/v1/v18_fixture/fixture_analysis/cohort/b88719ca15a200aebf0a79701c091ecdd229a7036fb03bba6b55683b88aebc85/summary/cohort.2e87cc7d7fcf4488.json','2e87cc7d7fcf4488c1306d0ae93194980226fa63255873f3d2ac7b4082e89a64','2e87cc7d7fcf4488',5);
CREATE TABLE request_bundle_projections (
            request_bundle_digest TEXT PRIMARY KEY
                CHECK(
                    length(request_bundle_digest) = 64
                    AND request_bundle_digest NOT GLOB '*[^0-9a-f]*'
                ),
            projection_json TEXT NOT NULL
        );
INSERT INTO "request_bundle_projections" VALUES('2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902','{"address":"entity_001","canonical_parameters":{},"determinism_contract":"deterministic","identity_contract_version":3,"namespace":"v18_fixture","output_contract":{"output_contract_version":1,"sibling_outputs":[{"declared_extension":".json","output_name":"source_value"}]},"result_affecting_settings":{},"role_labelled_bindings":[{"declared_extension":".txt","registered_content_digest":"45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9","registered_file_size":21,"role":"source_value","source_coordinate":{"context":"v18_fixture","entity_id":"entity_001","scope":"entity","source_name":"source_value"}}],"step_contract":{"callable_ref":"registry_v18_fixture_runtime:fixture_source_file","runner_contract_version":"2","step_contract_id":"fixture_source","step_contract_version":"1"}}');
INSERT INTO "request_bundle_projections" VALUES('31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2','{"address":"entity_001","canonical_parameters":{"variant":"base"},"determinism_contract":"deterministic","identity_contract_version":3,"namespace":"v18_fixture","output_contract":{"output_contract_version":1,"sibling_outputs":[{"declared_extension":".json","output_name":"left"},{"declared_extension":".json","output_name":"right"}]},"result_affecting_settings":{},"role_labelled_bindings":[{"output_name":"source_value","role":"source_value","upstream_request_bundle_digest":"2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902"}],"step_contract":{"callable_ref":"registry_v18_fixture_runtime:fixture_transform_file","runner_contract_version":"2","step_contract_id":"fixture_transform","step_contract_version":"1"}}');
INSERT INTO "request_bundle_projections" VALUES('b88719ca15a200aebf0a79701c091ecdd229a7036fb03bba6b55683b88aebc85','{"address":"cohort","canonical_parameters":{},"determinism_contract":"deterministic","identity_contract_version":3,"namespace":"v18_fixture","output_contract":{"output_contract_version":1,"sibling_outputs":[{"declared_extension":".json","output_name":"summary"}]},"result_affecting_settings":{},"role_labelled_bindings":[{"collection_semantics":"coordinate_set_v1","manifest_digest":"ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23","manifest_value_schema":"entity_set_v1","members":[{"output_name":"left","role":"left_values","upstream_request_bundle_digest":"31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2"}],"role":"left_values"},{"collection_semantics":"coordinate_set_v1","manifest_digest":"ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23","manifest_value_schema":"entity_set_v1","members":[{"output_name":"right","role":"right_values","upstream_request_bundle_digest":"31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2"}],"role":"right_values"}],"step_contract":{"callable_ref":"registry_v18_fixture_runtime:fixture_analysis_file","runner_contract_version":"2","step_contract_id":"fixture_analysis","step_contract_version":"1"}}');
CREATE TABLE run_execution_population (
            run_id INTEGER PRIMARY KEY
                REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            manifest_name TEXT NOT NULL,
            manifest_value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
        );
INSERT INTO "run_execution_population" VALUES(1,'cohort','entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23');
CREATE TABLE run_manifest_bindings (
            run_id INTEGER NOT NULL
                REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            step_name TEXT NOT NULL,
            manifest_usage_role TEXT NOT NULL,
            manifest_name TEXT NOT NULL,
            manifest_value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            PRIMARY KEY (run_id, step_name, manifest_usage_role),
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
        );
INSERT INTO "run_manifest_bindings" VALUES(1,'fixture_analysis','analysis_population','cohort','entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23');
CREATE TABLE workflow_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT NOT NULL,
            selected_step_name TEXT NOT NULL,
            selected_output_name TEXT NOT NULL,
            run_workspace TEXT NOT NULL,
            run_plan_path TEXT NOT NULL,
            run_plan_digest TEXT NOT NULL,
            base_workflow_name TEXT,
            resolution_summary_json TEXT NOT NULL,
            environment_observation_json TEXT NOT NULL,
            is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0, 1)),
            created_at TEXT NOT NULL
        );
INSERT INTO "workflow_runs" VALUES(1,'v18_fixture','base','fixture_analysis','summary','runs/v18_fixture/base/fixture_analysis','runs/v18_fixture/base/fixture_analysis/run_plan.json','e38407f0f44486ed6cd263bd6b7ef55487810955bfaf6f595eeedbde38f92d2b',NULL,'{"all_selected_resolved":true,"forced":false,"schema_version":1,"selected_outputs":[{"address":"cohort","context":"v18_fixture","output_name":"summary","resolution":{"artifact_id":5,"outcome":"generated"},"step_name":"fixture_analysis","workflow_name":"base"}]}','{"nipact_version":"0.0.1a13","platform":"Linux-6.17.9-76061709-generic-x86_64-with-glibc2.35","profile_version":1,"python_version":"3.12.7","snakemake_version":"9.14.6"}',1,'2026-08-13T21:06:38+00:00');
CREATE UNIQUE INDEX workflow_runs_current_scope_uq
            ON workflow_runs (
                context, workflow_name, selected_step_name, selected_output_name
            )
            WHERE is_current = 1;
CREATE UNIQUE INDEX artifacts_source_global_coordinate_uq
            ON artifacts(context, source_name)
            WHERE origin = 'source' AND source_scope = 'global';
CREATE UNIQUE INDEX artifacts_source_entity_coordinate_uq
            ON artifacts(context, source_name, source_entity_id)
            WHERE origin = 'source' AND source_scope = 'entity';
CREATE UNIQUE INDEX artifacts_workflow_output_uq
            ON artifacts(run_id, step_name, output_name, address)
            WHERE origin = 'workflow_output';
CREATE INDEX artifacts_path_idx
            ON artifacts(context, path);
CREATE INDEX artifacts_selected_lookup_idx
            ON artifacts(context, workflow_name, step_name, output_name, address)
            WHERE is_selected_output = 1;
CREATE INDEX artifacts_reuse_lookup_idx
            ON artifacts(
                context, step_name, address, request_bundle_digest, run_id
            )
            WHERE origin = 'workflow_output' AND is_published = 1;
CREATE INDEX published_outputs_artifact_id_idx
            ON published_outputs(artifact_id);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('artifacts',5);
INSERT INTO "sqlite_sequence" VALUES('workflow_runs',1);
INSERT INTO "sqlite_sequence" VALUES('parameters',4);
COMMIT;
