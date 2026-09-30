import subprocess, sys
STEPS=[
 'src.generators.generate_all',
 'src.ingestion.local_to_bronze',
 'src.transformations.bronze_to_silver',
 'src.quality.run_quality_checks',
 'src.transformations.build_gold',
]
for step in STEPS:
    print(f'\n=== Running {step} ===')
    subprocess.run([sys.executable,'-m',step],check=True)
print('\nPipeline completed successfully.')
