Below is the **exact, correct, safe workflow** you must follow **every time** you want to:

1. Modify a file
2. Save + commit it to **your branch** (`filippo/setup`)
3. Pull new files and updates from **Mattia’s main repo** (`upstream/main`)
4. **Never overwrite your work**


---

# ✅ **THE ONLY CORRECT WORKFLOW (copy this)**

## **STEP 1 — Start from your feature branch**

Always do your changes here, never on `main`:

```bash
git checkout filippo/setup
```

---

## **STEP 2 — Modify your file and save it normally**

You edit:

```
Lesson_XX.ipynb
```

Save with VS Code (`⌘S`).

---

## **STEP 3 — Stage and commit your changes**

```bash
git add<your file>
git commit -m "feat: update lesson XX"
```

Your work is now **safe** inside your branch.

---

## **STEP 4 — Update your `main` from the upstream repo**

This is the sync step — but NEVER do it while on your feature branch.

```bash
git checkout main
git fetch upstream
git merge upstream/main
git push origin main
```

Now your main is fully updated with:

* new files added by Mattia
* updated lessons
* bug fixes
* anything else in the official repo

**Nothing touches your edited branch.**

---

## **STEP 5 — Bring upstream updates into your feature branch**

```bash
git checkout filippo/setup
git merge upstream/main
```

### Important:

* If you changed lines that upstream didn’t change → merge is automatic.
* If you both changed the same exact lines → Git shows a conflict and **asks you what to keep**.
* Nothing is ever overwritten silently.

This ensures:

* Your work is preserved
* You also get all the “new files” from upstream
* Your branch stays compatible with the latest version of the repo

---

# ✔ Guaranteed behavior

Git will **not overwrite your files** on your branch **unless the exact same lines** were changed in the upstream main.
Even then, it produces a conflict — you choose.

Your branch **cannot lose files** if you follow this workflow.

---

# 🔒 The Golden Rule

```
NEVER edit files on main.
ALWAYS edit files on your feature branch.
ALWAYS update main first, then merge into your feature branch.
```

This is the only reliable workflow.

---

# 📌 TL;DR — PERFECT WORKFLOW

### When you want to edit something:

```bash
git checkout filippo/setup
# edit file
git add .
git commit -m "feat: update lesson XX"
```

### When you want the new files from upstream:

```bash
git checkout main
git fetch upstream
git merge upstream/main
git push origin main
```

### Then update your branch with the new stuff:

```bash
git checkout filippo/setup
git merge upstream/main
git push origin filippo/setup
```

---

