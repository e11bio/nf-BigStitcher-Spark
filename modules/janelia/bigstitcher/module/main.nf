process BIGSTITCHER_MODULE {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/bigstitcher:2.4.1-spark3.3.2-scala2.12-java17-ubuntu24.04' }
    cpus { spark.driver_cores }
    memory { spark.slurm_driver_memory }

    input:
    tuple val(meta), val(spark), val(module_class), val(module_args)
    path(data_files, stageAs: "?/*") // this is passed with the intention of mounting data files inside the container

    output:
    tuple val(meta), val(spark)

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = module_args ? module_args.join(' ') : ''
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def app_jar = '/app/app.jar'
    // extra driver JVM options, e.g. -Dmpicbg.models.TileUtil.seed=<long> to seed the
    // solver's tile shuffle, or -XX:ActiveProcessorCount=1 to make its schedule deterministic
    def extra_driver_java_options = task.ext.driver_java_options ? " ${task.ext.driver_java_options}" : ''
    """
    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        ${module_class}
        ${spark.parallelism}
        ${spark.worker_cores}
        ${executor_memory}
        ${spark.driver_cores}
        ${driver_memory}
        --spark-conf "spark.driver.extraClassPath=${app_jar}"
        --spark-conf "spark.executor.extraClassPath=${app_jar}"
        --spark-conf "spark.jars.ivy=\${SPARK_WORK_DIR}"
        --spark-conf "spark.driver.extraJavaOptions=-Dnative.libpath.verbose=true${extra_driver_java_options}"
        ${args}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}
