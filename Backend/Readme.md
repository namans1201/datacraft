# Backend Setup

Follow the steps below to run the backend locally:

### 1. Navigate to the backend directory
```bash
cd to/your/Backend/backend
```

### 2. Create a virtual environment
*For Mac -*
```zsh
python3 -m venv venv
```

*For Windows -*
```bash
python -m venv venv
```

### 3. Activate virtual environment
*For Mac -*
```zsh
source venv/bin/activate
```

*For Windows -*
```bash
venv\Scripts\activate
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Navigate to the root directory
```bash
cd to/your/Backend
```

### 6. Start the development server
```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```
