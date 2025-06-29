FROM australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest

# Writing an image in plotly requires the kaleido package.
# 1.0 requires Chrome and a plotly version that's incompatible with Hail.
RUN pip install 'kaleido<1.0'
