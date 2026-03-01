git rev-list --objects --all \
| git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
| sed -n 's/^blob //p' \
| sort -nrk2 \
| head -20

--------------

pip install git-filter-repogit filter-repo --path "chemin/du/fichier" --invert-pathsgit filter-repo --strip-blobs-bigger-than 100M

--------------

git reflog expire --expire=now --all
git gc --prune=now --aggressive

-----------------

git push origin --force --all
git push origin --force --tags


----------------


git fetch --all
git reset --hard origin/main
git clean -fd
