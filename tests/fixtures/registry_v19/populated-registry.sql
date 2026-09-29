BEGIN TRANSACTION;
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
INSERT INTO "artifact_dependencies" VALUES(2,1,'45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9',21,'.txt','../../../../../data/source/entity_001.txt','source_value','source_input',NULL,NULL,NULL,'entity','source_value','entity_001','data/source/entity_001.txt',NULL,NULL,NULL,1);
INSERT INTO "artifact_dependencies" VALUES(3,2,'887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8',63,'.json','staging/fixture_source/source_value/entity_001.json','source_value','source_input','fixture_source','source_value','entity_001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1);
INSERT INTO "artifact_dependencies" VALUES(4,2,'887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8',63,'.json','staging/fixture_source/source_value/entity_001.json','source_value','source_input','fixture_source','source_value','entity_001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1);
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
INSERT INTO "artifacts" VALUES(1,'source',NULL,'v18_fixture',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'data/source/entity_001.txt',0,0,NULL,NULL,'45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9','45e8f93b1f72302e',21,'.txt',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'entity','source_value','entity_001',66307,104494111,21,1789567990814234775,1790717782483802797,NULL,'2026-09-29T21:36:23+00:00');
INSERT INTO "artifacts" VALUES(2,'workflow_output',1,'v18_fixture','base','fixture_source','source_value','entity_001','job__fixture_source__source_value__entity_001',NULL,1,'outputs/v1/v18_fixture/fixture_source/entity_001/2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902/source_value/entity_001.887dedd0914cb8d4.json',0,1,'outputs/v1/v18_fixture/fixture_source/entity_001/2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902/source_value/entity_001.887dedd0914cb8d4.json','runs/v18_fixture/base/fixture_transform/left/staging/fixture_source/source_value/entity_001.json','887dedd0914cb8d46bf9ae1e6f37da8c2e611f9eabccfd9d55f5c58b399319d8','887dedd0914cb8d4',63,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_source_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2b3714abf567a946566f2a695895b7b8ed3112ddd7bf17455770c97413357902','2026-09-29T21:36:23+00:00');
INSERT INTO "artifacts" VALUES(3,'workflow_output',1,'v18_fixture','base','fixture_transform','left','entity_001','job__fixture_transform__outputs__entity_001',NULL,2,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/left/entity_001.8167bac79d7f1a3e.json',1,1,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/left/entity_001.8167bac79d7f1a3e.json','runs/v18_fixture/base/fixture_transform/left/staging/fixture_transform/left/entity_001.json','8167bac79d7f1a3e14b1778b583adfe20bcfc9f44a202144ac194313d3b74fed','8167bac79d7f1a3e',94,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_transform_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2','2026-09-29T21:36:23+00:00');
INSERT INTO "artifacts" VALUES(4,'workflow_output',1,'v18_fixture','base','fixture_transform','right','entity_001','job__fixture_transform__outputs__entity_001',NULL,2,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/right/entity_001.ae27d728c8e2eb89.json',0,1,'outputs/v1/v18_fixture/fixture_transform/entity_001/31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2/right/entity_001.ae27d728c8e2eb89.json','runs/v18_fixture/base/fixture_transform/left/staging/fixture_transform/right/entity_001.json','ae27d728c8e2eb89f88d0e84f7984de963fca681b9252c67847ee59b8d711c07','ae27d728c8e2eb89',95,'.json',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'registry_v18_fixture_runtime:fixture_transform_file',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'31835e798e67b2943737f4b0df1c60bc00a30253a20ffaffaf170076ff1b5df2','2026-09-29T21:36:23+00:00');
CREATE TABLE contexts (
            context TEXT PRIMARY KEY,
            runtime_path TEXT NOT NULL,
            storage_layout_version INTEGER NOT NULL DEFAULT 1 CHECK(
                storage_layout_version = 1
            )
        );
INSERT INTO "contexts" VALUES('v18_fixture','/home/forest/dev/nipact/tmp/directory-outputs/pre-feature/runtime',1);
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
INSERT INTO "parameters" VALUES(1,1,'44136fa355b3678a','44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a','fixture_source','{}','2026-09-29T21:36:23+00:00');
INSERT INTO "parameters" VALUES(2,1,'5deeee15ee938f3f','5deeee15ee938f3f5f689a10dcf11137d9963ae23f24ec543967fbc6d34c7de3','fixture_transform','{"variant":"base"}','2026-09-29T21:36:23+00:00');
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
INSERT INTO "run_execution_population" VALUES(2,'cohort','entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23');
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
CREATE TABLE specification_attempt_results (
        attempt_id INTEGER NOT NULL,
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        role TEXT NOT NULL,
        address TEXT NOT NULL,
        artifact_id INTEGER NOT NULL
            REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
        PRIMARY KEY (attempt_id, role, address),
        FOREIGN KEY (attempt_id, snapshot_digest, member_key)
            REFERENCES specification_member_attempts(
                attempt_id, snapshot_digest, member_key
            ) ON DELETE RESTRICT,
        FOREIGN KEY (snapshot_digest, member_key, role, address)
            REFERENCES specification_expected_results(
                snapshot_digest, member_key, role, address
            ) ON DELETE RESTRICT
    );
INSERT INTO "specification_attempt_results" VALUES(1,'ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','left','entity_001',3);
INSERT INTO "specification_attempt_results" VALUES(1,'ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','right','entity_001',4);
CREATE TABLE specification_expected_results (
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        role TEXT NOT NULL CHECK(length(trim(role)) > 0),
        step_name TEXT NOT NULL CHECK(length(trim(step_name)) > 0),
        output_name TEXT NOT NULL CHECK(length(trim(output_name)) > 0),
        address TEXT NOT NULL CHECK(length(trim(address)) > 0),
        PRIMARY KEY (snapshot_digest, member_key, role, address),
        UNIQUE (
            snapshot_digest, member_key, step_name, output_name, address
        ),
        FOREIGN KEY (snapshot_digest, member_key)
            REFERENCES specification_members(snapshot_digest, member_key)
            ON DELETE RESTRICT
    );
INSERT INTO "specification_expected_results" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','left','fixture_transform','left','entity_001');
INSERT INTO "specification_expected_results" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','right','fixture_transform','right','entity_001');
INSERT INTO "specification_expected_results" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000002','left','fixture_transform','left','entity_001');
INSERT INTO "specification_expected_results" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000002','right','fixture_transform','right','entity_001');
CREATE TABLE specification_member_attempts (
        attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        started_at TEXT NOT NULL CHECK(length(trim(started_at)) > 0),
        finished_at TEXT CHECK(
            finished_at IS NULL OR length(trim(finished_at)) > 0
        ),
        outcome TEXT CHECK(
            outcome IS NULL OR outcome IN ('failed', 'partial', 'complete')
        ),
        selecting_run_id INTEGER UNIQUE
            REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
        failure_stage TEXT CHECK(
            failure_stage IS NULL
            OR (
                length(trim(failure_stage)) > 0
                AND length(failure_stage) <= 64
            )
        ),
        failure_summary TEXT CHECK(
            failure_summary IS NULL
            OR (
                length(trim(failure_summary)) > 0
                AND length(failure_summary) <= 4096
            )
        ),
        UNIQUE (attempt_id, snapshot_digest, member_key),
        FOREIGN KEY (snapshot_digest, member_key)
            REFERENCES specification_members(snapshot_digest, member_key)
            ON DELETE RESTRICT,
        CHECK (
            (
                outcome IS NULL
                AND finished_at IS NULL
                AND selecting_run_id IS NULL
                AND failure_stage IS NULL
                AND failure_summary IS NULL
            )
            OR
            (
                outcome IN ('partial', 'complete')
                AND finished_at IS NOT NULL
                AND selecting_run_id IS NOT NULL
                AND failure_stage IS NULL
                AND failure_summary IS NULL
            )
            OR
            (
                outcome = 'failed'
                AND finished_at IS NOT NULL
                AND failure_stage IS NOT NULL
                AND failure_summary IS NOT NULL
            )
        )
    );
INSERT INTO "specification_member_attempts" VALUES(1,'ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','2026-09-29T21:36:22+00:00','2026-09-29T21:36:23+00:00','complete',1,NULL,NULL);
CREATE TABLE specification_members (
        snapshot_digest TEXT NOT NULL
            REFERENCES specification_snapshots(snapshot_digest) ON DELETE RESTRICT,
        member_key TEXT NOT NULL CHECK(length(trim(member_key)) > 0),
        row_digest TEXT NOT NULL CHECK(
            length(row_digest) = 64
            AND row_digest NOT GLOB '*[^0-9a-f]*'
        ),
        disposition TEXT NOT NULL CHECK(
            disposition IN ('included', 'excluded')
        ),
        exclusion_reason TEXT,
        PRIMARY KEY (snapshot_digest, member_key),
        UNIQUE (snapshot_digest, row_digest),
        CHECK (
            (disposition = 'included' AND exclusion_reason IS NULL)
            OR
            (
                disposition = 'excluded'
                AND exclusion_reason IS NOT NULL
                AND length(trim(exclusion_reason)) > 0
            )
        )
    );
INSERT INTO "specification_members" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000001','98bd1e27a03125b40a9a8fc8ed198ea5f508c66c9c26b1b4460b919ab12e1352','included',NULL);
INSERT INTO "specification_members" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','member-000002','254ea67fb5e4d2c1f69762f5dac8f9057f105afeefdcc95f5877a0d172a6017c','excluded','Prospectively excluded compact case.');
CREATE TABLE specification_snapshot_manifest_values (
        snapshot_digest TEXT NOT NULL
            REFERENCES specification_snapshots(snapshot_digest) ON DELETE RESTRICT,
        value_schema TEXT NOT NULL CHECK(length(trim(value_schema)) > 0),
        manifest_digest TEXT NOT NULL,
        PRIMARY KEY (snapshot_digest, value_schema, manifest_digest),
        FOREIGN KEY (value_schema, manifest_digest)
            REFERENCES manifest_values(value_schema, manifest_digest)
            ON DELETE RESTRICT
    );
INSERT INTO "specification_snapshot_manifest_values" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','entity_set_v1','ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23');
CREATE TABLE specification_snapshots (
        snapshot_digest TEXT PRIMARY KEY CHECK(
            length(snapshot_digest) = 64
            AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
        ),
        context TEXT NOT NULL
            REFERENCES contexts(context) ON DELETE RESTRICT,
        canonical_bytes BLOB NOT NULL CHECK(
            typeof(canonical_bytes) = 'blob'
            AND length(canonical_bytes) > 0
        )
    );
INSERT INTO "specification_snapshots" VALUES('ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d','v18_fixture',X'7B22636F6E74657874223A227631385F66697874757265222C226D616E69666573745F76616C756573223A5B7B2263616E6F6E6963616C5F626F6479223A22656E746974795F303031222C226D616E69666573745F646967657374223A2266666666656132613864636634346434646231663534643364643336663937353562323263353465303439313937343565323239353662623062316438633233222C2276616C75655F736368656D61223A22656E746974795F7365745F7631227D5D2C226D656D62657273223A5B7B22646973706F736974696F6E223A22696E636C75646564222C226578636C7573696F6E5F726561736F6E223A6E756C6C2C226D656D6265725F6B6579223A226D656D6265722D303030303031222C22726F77223A7B226465636973696F6E5F636F6F7264696E61746573223A5B7B226E616D65223A2276617269616E74222C2276616C7565223A2262617365227D5D2C226566666563746976655F6465636C61726174696F6E223A7B22657865637574696F6E5F706F70756C6174696F6E223A7B226D616E69666573745F646967657374223A2266666666656132613864636634346434646231663534643364643336663937353562323263353465303439313937343565323239353662623062316438633233222C226D616E69666573745F6E616D65223A22636F686F7274222C226D616E69666573745F76616C75655F736368656D61223A22656E746974795F7365745F7631227D2C22726573756C7473223A5B7B22616464726573735F73636F7065223A22656E74697479222C226F75747075745F6E616D65223A226C656674222C22726F6C65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D2C7B22616464726573735F73636F7065223A22656E74697479222C226F75747075745F6E616D65223A227269676874222C22726F6C65223A227269676874222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D5D2C227374657073223A5B7B22616464726573735F73636F7065223A22656E74697479222C2263616C6C61626C655F726566223A2272656769737472795F7631385F666978747572655F72756E74696D653A666978747572655F736F757263655F66696C65222C22657865637574696F6E5F726F6C65223A22736F757263655F696D706F7274222C22696E70757473223A5B5D2C226D616E69666573745F62696E64696E67223A6E756C6C2C226F757470757473223A5B7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A22736F757263655F76616C7565227D5D2C22706172616D73223A7B7D2C227061747465726E5F6B696E64223A227061747465726E5F61222C22736F757263655F696E70757473223A5B22736F757263655F76616C7565225D2C22737465705F636F6E74726163745F76657273696F6E223A2231222C22737465705F6E616D65223A22666978747572655F736F75726365227D2C7B22616464726573735F73636F7065223A22656E74697479222C2263616C6C61626C655F726566223A2272656769737472795F7631385F666978747572655F72756E74696D653A666978747572655F7472616E73666F726D5F66696C65222C22657865637574696F6E5F726F6C65223A227472616E73666F726D222C22696E70757473223A5B7B22646570656E64656E63795F726F6C65223A22736F757263655F696E707574222C226E616D65223A22736F757263655F76616C7565222C22736F757263655F6F75747075745F6E616D65223A22736F757263655F76616C7565222C22736F757263655F737465705F6E616D65223A22666978747572655F736F75726365227D5D2C226D616E69666573745F62696E64696E67223A6E756C6C2C226F757470757473223A5B7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A226C656674227D2C7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A227269676874227D5D2C22706172616D73223A7B2276617269616E74223A2262617365227D2C227061747465726E5F6B696E64223A227061747465726E5F61222C22736F757263655F696E70757473223A5B5D2C22737465705F636F6E74726163745F76657273696F6E223A2231222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D5D2C22746172676574223A7B226F75747075745F6E616D65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D7D2C22736368656D61223A226E69706163742F73706563696669636174696F6E2D726F772F7631222C22776F726B666C6F775F73656C6563746F72223A2262617365222C22777269746573223A5B7B22706172616D657465725F6E616D65223A2276617269616E74222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22706172616D65746572222C2276616C7565223A2262617365227D2C7B226D616E69666573745F6E616D65223A22636F686F7274222C2274797065223A22657865637574696F6E5F706F70756C6174696F6E227D2C7B226F75747075745F6E616D65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22746172676574227D2C7B226F75747075745F6E616D65223A226C656674222C22726F6C65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22726573756C74227D2C7B226F75747075745F6E616D65223A227269676874222C22726F6C65223A227269676874222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22726573756C74227D5D7D2C22726F775F646967657374223A2239386264316532376130333132356234306139613866633865643139386561356635303863363663396332366231623434363062393139616231326531333532227D2C7B22646973706F736974696F6E223A226578636C75646564222C226578636C7573696F6E5F726561736F6E223A2250726F73706563746976656C79206578636C7564656420636F6D7061637420636173652E222C226D656D6265725F6B6579223A226D656D6265722D303030303032222C22726F77223A7B226465636973696F6E5F636F6F7264696E61746573223A5B7B226E616D65223A2276617269616E74222C2276616C7565223A226368616E676564227D5D2C226566666563746976655F6465636C61726174696F6E223A7B22657865637574696F6E5F706F70756C6174696F6E223A7B226D616E69666573745F646967657374223A2266666666656132613864636634346434646231663534643364643336663937353562323263353465303439313937343565323239353662623062316438633233222C226D616E69666573745F6E616D65223A22636F686F7274222C226D616E69666573745F76616C75655F736368656D61223A22656E746974795F7365745F7631227D2C22726573756C7473223A5B7B22616464726573735F73636F7065223A22656E74697479222C226F75747075745F6E616D65223A226C656674222C22726F6C65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D2C7B22616464726573735F73636F7065223A22656E74697479222C226F75747075745F6E616D65223A227269676874222C22726F6C65223A227269676874222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D5D2C227374657073223A5B7B22616464726573735F73636F7065223A22656E74697479222C2263616C6C61626C655F726566223A2272656769737472795F7631385F666978747572655F72756E74696D653A666978747572655F736F757263655F66696C65222C22657865637574696F6E5F726F6C65223A22736F757263655F696D706F7274222C22696E70757473223A5B5D2C226D616E69666573745F62696E64696E67223A6E756C6C2C226F757470757473223A5B7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A22736F757263655F76616C7565227D5D2C22706172616D73223A7B7D2C227061747465726E5F6B696E64223A227061747465726E5F61222C22736F757263655F696E70757473223A5B22736F757263655F76616C7565225D2C22737465705F636F6E74726163745F76657273696F6E223A2231222C22737465705F6E616D65223A22666978747572655F736F75726365227D2C7B22616464726573735F73636F7065223A22656E74697479222C2263616C6C61626C655F726566223A2272656769737472795F7631385F666978747572655F72756E74696D653A666978747572655F7472616E73666F726D5F66696C65222C22657865637574696F6E5F726F6C65223A227472616E73666F726D222C22696E70757473223A5B7B22646570656E64656E63795F726F6C65223A22736F757263655F696E707574222C226E616D65223A22736F757263655F76616C7565222C22736F757263655F6F75747075745F6E616D65223A22736F757263655F76616C7565222C22736F757263655F737465705F6E616D65223A22666978747572655F736F75726365227D5D2C226D616E69666573745F62696E64696E67223A6E756C6C2C226F757470757473223A5B7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A226C656674227D2C7B22616464726573735F73636F7065223A22656E74697479222C22657874656E73696F6E223A222E6A736F6E222C226E616D65223A227269676874227D5D2C22706172616D73223A7B2276617269616E74223A226368616E676564227D2C227061747465726E5F6B696E64223A227061747465726E5F61222C22736F757263655F696E70757473223A5B5D2C22737465705F636F6E74726163745F76657273696F6E223A2231222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D5D2C22746172676574223A7B226F75747075745F6E616D65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D227D7D2C22736368656D61223A226E69706163742F73706563696669636174696F6E2D726F772F7631222C22776F726B666C6F775F73656C6563746F72223A2262617365222C22777269746573223A5B7B22706172616D657465725F6E616D65223A2276617269616E74222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22706172616D65746572222C2276616C7565223A226368616E676564227D2C7B226D616E69666573745F6E616D65223A22636F686F7274222C2274797065223A22657865637574696F6E5F706F70756C6174696F6E227D2C7B226F75747075745F6E616D65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22746172676574227D2C7B226F75747075745F6E616D65223A226C656674222C22726F6C65223A226C656674222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22726573756C74227D2C7B226F75747075745F6E616D65223A227269676874222C22726F6C65223A227269676874222C22737465705F6E616D65223A22666978747572655F7472616E73666F726D222C2274797065223A22726573756C74227D5D7D2C22726F775F646967657374223A2232353465613637666235653464326331663639373632663564616338663930353766313035616665656664636339356635383737613064313732613630313763227D5D2C22736368656D61223A226E69706163742F73706563696669636174696F6E2D736E617073686F742F7631227D');
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
INSERT INTO "workflow_runs" VALUES(1,'v18_fixture','base','fixture_transform','left','runs/v18_fixture/base/fixture_transform/left','runs/v18_fixture/base/fixture_transform/left/run_plan.json','d8eb32732751264b7a497d6f228e4d2a6060d675f491595002ab4cbb7a1e689a',NULL,'{"all_selected_resolved":true,"forced":false,"schema_version":1,"selected_outputs":[{"address":"entity_001","context":"v18_fixture","output_name":"left","resolution":{"artifact_id":3,"outcome":"generated"},"step_name":"fixture_transform","workflow_name":"base"}]}','{"nipact_version":"0.0.1a15","platform":"Linux-6.17.9-76061709-generic-x86_64-with-glibc2.35","profile_version":1,"python_version":"3.12.7","snakemake_version":"9.14.6"}',0,'2026-09-29T21:36:23+00:00');
INSERT INTO "workflow_runs" VALUES(2,'v18_fixture','base','fixture_transform','left','runs/v18_fixture/base/fixture_transform/left','runs/v18_fixture/base/fixture_transform/left/run_plan.json','d634f1de8a139547622d2d8adf80644f35ac99a39da25d02fe77f167750597cf',NULL,'{"all_selected_resolved":true,"forced":false,"schema_version":1,"selected_outputs":[{"address":"entity_001","context":"v18_fixture","output_name":"left","resolution":{"artifact_id":3,"outcome":"reused"},"step_name":"fixture_transform","workflow_name":"base"}]}','{"nipact_version":"0.0.1a15","platform":"Linux-6.17.9-76061709-generic-x86_64-with-glibc2.35","profile_version":1,"python_version":"3.12.7","snakemake_version":"9.14.6"}',1,'2026-09-29T21:36:23+00:00');
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
CREATE INDEX specification_snapshot_manifest_values_value_idx
        ON specification_snapshot_manifest_values (
            value_schema, manifest_digest, snapshot_digest
        );
CREATE INDEX specification_member_attempts_member_idx
        ON specification_member_attempts (
            snapshot_digest, member_key, attempt_id
        );
CREATE INDEX specification_attempt_results_artifact_idx
        ON specification_attempt_results (artifact_id, attempt_id);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('specification_member_attempts',1);
INSERT INTO "sqlite_sequence" VALUES('artifacts',4);
INSERT INTO "sqlite_sequence" VALUES('workflow_runs',2);
INSERT INTO "sqlite_sequence" VALUES('parameters',3);
COMMIT;
