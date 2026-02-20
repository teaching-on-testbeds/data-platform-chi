all: \
	index.md \
	0_intro.ipynb \
	1_create_server.ipynb \
	2_application.ipynb \
	3_endpoints.ipynb \
	4_realtime.ipynb \
	5_iceberg.ipynb \
	6_training.ipynb \
	7_delete.ipynb

clean:
	rm -f index.md \
	0_intro.ipynb \
	1_create_server.ipynb \
	2_application.ipynb \
	3_endpoints.ipynb \
	4_realtime.ipynb \
	5_iceberg.ipynb \
	6_training.ipynb \
	7_delete.ipynb

index.md: snippets/*.md
	cat snippets/intro.md \
		snippets/create_server.md \
		snippets/application.md \
		snippets/endpoints.md \
		snippets/realtime.md \
		snippets/iceberg.md \
		snippets/training.md \
		snippets/delete.md \
		snippets/footer.md \
		> index.tmp.md
	grep -v '^:::' index.tmp.md > index.md
	rm index.tmp.md

0_intro.ipynb: snippets/intro.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/intro.md \
				-o 0_intro.ipynb
	sed -i 's/attachment://g' 0_intro.ipynb

1_create_server.ipynb: snippets/create_server.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/create_server.md \
				-o 1_create_server.ipynb
	sed -i 's/attachment://g' 1_create_server.ipynb

2_application.ipynb: snippets/application.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/application.md \
				-o 2_application.ipynb
	sed -i 's/attachment://g' 2_application.ipynb

3_endpoints.ipynb: snippets/endpoints.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/endpoints.md \
				-o 3_endpoints.ipynb
	sed -i 's/attachment://g' 3_endpoints.ipynb

4_realtime.ipynb: snippets/realtime.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/realtime.md \
				-o 4_realtime.ipynb
	sed -i 's/attachment://g' 4_realtime.ipynb

5_iceberg.ipynb: snippets/iceberg.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/iceberg.md \
				-o 5_iceberg.ipynb
	sed -i 's/attachment://g' 5_iceberg.ipynb

6_training.ipynb: snippets/training.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/training.md \
				-o 6_training.ipynb
	sed -i 's/attachment://g' 6_training.ipynb

7_delete.ipynb: snippets/delete.md
	pandoc --resource-path=../ --embed-resources --standalone --wrap=none \
				-i snippets/frontmatter_python.md snippets/delete.md \
				-o 7_delete.ipynb
	sed -i 's/attachment://g' 7_delete.ipynb
